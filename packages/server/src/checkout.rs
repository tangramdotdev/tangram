use crate::Server;
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream};
use num::ToPrimitive as _;
use reflink_copy::reflink;
use std::{
	collections::{HashMap, HashSet},
	os::unix::fs::PermissionsExt as _,
	panic::AssertUnwindSafe,
	path::PathBuf,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::{Ext as _, TryExt as _};
use tangram_http::{Body, request::Ext as _};
use tokio_util::{io::InspectReader, task::AbortOnDropHandle};

struct State {
	arg: tg::checkout::Arg,
	artifact: tg::artifact::Id,
	artifacts_path: Option<PathBuf>,
	artifacts_path_created: bool,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, fnv::FnvBuildHasher>,
	path: PathBuf,
	progress: crate::progress::Handle<tg::checkout::Output>,
	visited: HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug, Default)]
struct Progress {
	objects: u64,
	bytes: u64,
}

impl Server {
	pub async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		if arg.path.is_none() {
			let path = self.artifacts_path().join(arg.artifact.to_string());
			if self.vfs.lock().unwrap().is_none() {
				let cache_arg = tg::cache::Arg {
					artifacts: vec![arg.artifact.clone()],
				};
				let stream = self.cache(cache_arg).await?;
				return Ok(stream
					.map_ok({
						let server = self.clone();
						move |event| {
							event.map_output(|()| {
								let path = server.artifacts_path().join(arg.artifact.to_string());
								tg::checkout::Output { path }
							})
						}
					})
					.left_stream()
					.left_stream());
			}
			return Ok(stream::once(future::ok(tg::progress::Event::Output(
				tg::checkout::Output { path },
			)))
			.right_stream()
			.left_stream());
		}
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let artifact = arg.artifact.clone();
			let arg = arg.clone();
			let progress = progress.clone();
			async move {
				// Ensure the artifact is complete.
				let result = server
					.checkout_ensure_complete(&artifact, &progress)
					.await
					.map_err(
						|source| tg::error!(!source, %artifact, "failed to ensure the artifact is complete"),
					);
				if let Err(error) = result {
					tracing::warn!(?error);
					progress.log(
						tg::progress::Level::Warning,
						"failed to ensure the artifact is complete".into(),
					);
				}

				progress.spinner("checkout", "checkout");
				let metadata = server
					.try_get_object_metadata(&arg.artifact.clone().into())
					.await
					.ok()
					.flatten();
				let count = metadata.as_ref().and_then(|metadata| metadata.count);
				let weight = metadata.as_ref().and_then(|metadata| metadata.weight);
				progress.start(
					"objects".to_owned(),
					"objects".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					count,
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					weight,
				);

				let result = AssertUnwindSafe(server.checkout_task(artifact, arg, &progress))
					.catch_unwind()
					.await;

				progress.finish_all();

				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle).right_stream();
		Ok(stream)
	}

	pub(crate) async fn checkout_ensure_complete(
		&self,
		artifact: &tg::artifact::Id,
		progress: &crate::progress::Handle<tg::checkout::Output>,
	) -> tg::Result<()> {
		// Check if the artifact is complete.
		let complete = self
			.try_get_object_complete_local(&artifact.clone().into())
			.await?
			.unwrap_or_default();
		if complete {
			return Ok(());
		}

		// Create a future to pull the artifact.
		let pull_future = {
			let progress = progress.clone();
			let server = self.clone();
			async move {
				let stream = server
					.pull(tg::pull::Arg {
						items: vec![Either::Right(artifact.clone().into())],
						remote: Some("default".to_owned()),
						..tg::pull::Arg::default()
					})
					.await?;
				progress.spinner("pull", "pull");
				let mut stream = std::pin::pin!(stream);
				while let Some(event) = stream.try_next().await? {
					progress.forward(Ok(event));
				}
				Ok::<_, tg::Error>(())
			}
		}
		.boxed();

		// Create a future to index then check if the artifact is complete.
		let index_future = {
			let artifact = artifact.clone();
			let server = self.clone();
			async move {
				let stream = server.index().await?;
				let stream = std::pin::pin!(stream);
				stream.try_last().await?;
				let complete = server
					.try_get_object_complete_local(&artifact.clone().into())
					.await?
					.ok_or_else(|| tg::error!(%artifact, "expected an object"))?;
				if !complete {
					return Err(tg::error!("expected the object to be complete"));
				}
				Ok::<_, tg::Error>(())
			}
		}
		.boxed();

		// Select the pull and index futures.
		future::select_ok([pull_future, index_future]).await?;

		progress.finish_all();

		Ok(())
	}

	async fn checkout_task(
		&self,
		artifact: tg::artifact::Id,
		arg: tg::checkout::Arg,
		progress: &crate::progress::Handle<tg::checkout::Output>,
	) -> tg::Result<tg::checkout::Output> {
		// Get the path.
		let path = arg
			.path
			.clone()
			.ok_or_else(|| tg::error!("expected the path to be set"))?;

		// Canonicalize the path's parent.
		let path = crate::util::fs::canonicalize_parent(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path's parent"))?;

		// Determine the artifacts path.
		let artifacts_path = if artifact.is_directory() {
			Some(path.join(".tangram/artifacts"))
		} else {
			None
		};

		// Check if an artifact exists at the path.
		let exists = tokio::fs::try_exists(&path).await.unwrap_or(false);

		// If an artifact exists, and this is not a forced checkout, then return an error.
		if exists && !arg.force {
			return Err(tg::error!(
				"there is an existing file system object at the path"
			));
		}

		let task = tokio::task::spawn_blocking({
			let server = self.clone();
			let path = path.clone();
			let progress = progress.clone();
			move || {
				// Create the state.
				let mut state = State {
					arg,
					artifact,
					artifacts_path,
					artifacts_path_created: false,
					graphs: HashMap::default(),
					path,
					progress,
					visited: HashSet::default(),
				};

				// Check out the artifact.
				let id = state.artifact.clone();
				let artifact = Either::Right(id.clone());
				let path = state.path.clone();
				server.checkout_inner(&mut state, path, &id, artifact)?;

				// Write the lockfile if necessary.
				server.checkout_write_lockfile(id, &mut state)?;

				Ok::<_, tg::Error>(())
			}
		});
		let abort_handle = task.abort_handle();
		scopeguard::defer! {
			abort_handle.abort();
		}

		// Delete the partially constructed output if checkout failed.
		if let Err(error) = task.await.unwrap() {
			crate::util::fs::remove(&path).await.ok();
			return Err(error);
		}

		let output = tg::checkout::Output { path };

		Ok(output)
	}

	fn checkout_dependency(
		&self,
		state: &mut State,
		id: &tg::artifact::Id,
		artifact: Either<(tg::graph::Id, usize), tg::artifact::Id>,
	) -> tg::Result<()> {
		if !state.arg.dependencies {
			return Ok(());
		}
		if !state.visited.insert(id.clone()) {
			return Ok(());
		}
		let artifacts_path = state
			.artifacts_path
			.as_ref()
			.ok_or_else(|| tg::error!("cannot check out a dependency without an artifacts path"))?;
		if !state.artifacts_path_created {
			std::fs::create_dir_all(artifacts_path).map_err(|source| {
				tg::error!(!source, "failed to create the artifacts directory")
			})?;
			state.artifacts_path_created = true;
		}
		let path = artifacts_path.join(id.to_string());
		self.checkout_inner(state, path, id, artifact)?;
		Ok(())
	}

	fn checkout_inner(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		artifact: Either<(tg::graph::Id, usize), tg::artifact::Id>,
	) -> tg::Result<()> {
		// Get the graph or artifact's data.
		let data = match artifact {
			// If the artifact refers to a graph, then add it to the state.
			Either::Left((graph, node)) => {
				if !state.graphs.contains_key(&graph) {
					#[allow(clippy::match_wildcard_for_single_variants)]
					let data = match &self.store {
						crate::Store::Lmdb(store) => {
							store.try_get_object_data_sync(&graph.clone().into())?
						},
						crate::Store::Memory(store) => {
							store.try_get_object_data(&graph.clone().into())?
						},
						_ => return Err(tg::error!("not yet implemented")),
					}
					.ok_or_else(|| tg::error!("failed to get the value"))?
					.try_into()
					.map_err(|_| tg::error!("expected a graph"))?;
					state.graphs.insert(graph.clone(), data);
				}
				Either::Left((graph, node))
			},

			// Otherwise, get the artifact's data.
			Either::Right(id) => {
				#[allow(clippy::match_wildcard_for_single_variants)]
				let data = match &self.store {
					crate::Store::Lmdb(store) => store.try_get_object_data_sync(&id.into())?,
					crate::Store::Memory(store) => store.try_get_object_data(&id.into())?,
					_ => return Err(tg::error!("not yet implemented")),
				}
				.ok_or_else(|| tg::error!("failed to get the value"))?;
				let data = tg::artifact::Data::try_from(data)?;
				Either::Right(data)
			},
		};

		// Check out the artifact.
		match data {
			Either::Left((graph, node)) => {
				self.checkout_inner_graph(state, path, id, &graph, node)?;
			},
			Either::Right(data) => {
				self.checkout_inner_data(state, path, id, data)?;
			},
		}
		state.progress.increment("objects", 1);
		Ok(())
	}

	fn checkout_inner_graph(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		graph: &tg::graph::Id,
		node: usize,
	) -> tg::Result<()> {
		let data = state
			.graphs
			.get(graph)
			.ok_or_else(|| tg::error!("expected the graph to exist"))?;
		let weight = data.serialize().unwrap().len().to_u64().unwrap();
		let node = data
			.nodes
			.get(node)
			.ok_or_else(|| tg::error!("expected the node to exist"))?
			.clone();
		match node {
			tg::graph::data::Node::Directory(directory) => {
				self.checkout_inner_graph_directory(state, path, id, graph, &directory)?;
			},
			tg::graph::data::Node::File(file) => {
				self.checkout_inner_graph_file(state, path, id, graph, file)?;
			},
			tg::graph::data::Node::Symlink(symlink) => {
				self.checkout_inner_graph_symlink(state, path, id, graph, symlink)?;
			},
		}
		state.progress.increment("bytes", weight);
		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn checkout_inner_graph_directory(
		&self,
		state: &mut State,
		path: PathBuf,
		_id: &tg::artifact::Id,
		graph: &tg::graph::Id,
		directory: &tg::graph::data::Directory,
	) -> tg::Result<()> {
		std::fs::create_dir_all(&path).unwrap();
		for (name, artifact) in &directory.entries {
			let id = match artifact.clone() {
				Either::Left(node) => {
					let kind = state
						.graphs
						.get(graph)
						.unwrap()
						.nodes
						.get(node)
						.ok_or_else(|| tg::error!("expected the node to exist"))?
						.kind();
					let graph = graph.clone();
					let data: tg::artifact::Data = match kind {
						tg::artifact::Kind::Directory => {
							tg::directory::Data::Graph(tg::directory::data::Graph {
								graph: graph.clone(),
								node,
							})
							.into()
						},
						tg::artifact::Kind::File => tg::file::Data::Graph(tg::file::data::Graph {
							graph: graph.clone(),
							node,
						})
						.into(),
						tg::artifact::Kind::Symlink => {
							tg::symlink::Data::Graph(tg::symlink::data::Graph {
								graph: graph.clone(),
								node,
							})
							.into()
						},
					};
					let bytes = data.serialize()?;
					tg::artifact::Id::new(kind, &bytes)
				},
				Either::Right(id) => id.clone(),
			};
			let artifact = artifact.clone().map_left(|node| (graph.clone(), node));
			let path = path.join(name);
			self.checkout_inner(state, path, &id, artifact)?;
		}
		Ok(())
	}

	fn checkout_inner_graph_file(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		graph: &tg::graph::Id,
		file: tg::graph::data::File,
	) -> tg::Result<()> {
		let tg::graph::data::File {
			contents,
			dependencies,
			executable,
		} = file;

		// Check out the dependencies.
		for referent in dependencies.values() {
			let id = match referent.item.clone() {
				Either::Left(node) => {
					let kind = state
						.graphs
						.get(graph)
						.unwrap()
						.nodes
						.get(node)
						.ok_or_else(|| tg::error!("expected the node to exist"))?
						.kind();
					let graph = graph.clone();
					let data: tg::artifact::Data = match kind {
						tg::artifact::Kind::Directory => {
							tg::directory::Data::Graph(tg::directory::data::Graph {
								graph: graph.clone(),
								node,
							})
							.into()
						},
						tg::artifact::Kind::File => tg::file::Data::Graph(tg::file::data::Graph {
							graph: graph.clone(),
							node,
						})
						.into(),
						tg::artifact::Kind::Symlink => {
							tg::symlink::Data::Graph(tg::symlink::data::Graph {
								graph: graph.clone(),
								node,
							})
							.into()
						},
					};
					let bytes = data.serialize()?;
					tg::artifact::Id::new(kind, &bytes)
				},
				Either::Right(id) => match tg::artifact::Id::try_from(id.clone()) {
					Ok(id) => id,
					Err(_) => continue,
				},
			};
			if id != state.artifact {
				let artifact = match &referent.item {
					Either::Left(node) => Either::Left((graph.clone(), *node)),
					Either::Right(id) => match tg::artifact::Id::try_from(id.clone()) {
						Ok(id) => Either::Right(id),
						Err(_) => continue,
					},
				};
				self.checkout_dependency(state, &id, artifact)?;
			}
		}

		// Copy the file.
		let src = &self.cache_path().join(id.to_string());
		let dst = &path;
		let mut done = false;
		let mut error = None;
		if !done {
			let size = std::fs::metadata(src)
				.ok()
				.map_or(0, |metadata| metadata.len());
			let result = reflink(src, dst);
			match result {
				Ok(()) => {
					done = true;
					state.progress.increment("bytes", size);
				},
				Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
					done = true;
					state.progress.increment("bytes", size);
				},
				Err(error_) => {
					error = Some(error_);
				},
			}
		}
		if !done {
			let server = self.clone();
			let path = path.clone();
			let future = async move {
				let reader = tg::Blob::with_id(contents)
					.read(&server, tg::blob::read::Arg::default())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
				let mut reader = InspectReader::new(reader, {
					let progress = state.progress.clone();
					move |buf| {
						progress.increment("bytes", buf.len().to_u64().unwrap());
					}
				});
				let mut file_ = tokio::fs::File::create(&path)
					.await
					.map_err(|source| tg::error!(!source, ?path, "failed to create the file"))?;
				tokio::io::copy(&mut reader, &mut file_).await.map_err(
					|source| tg::error!(!source, ?path = path, "failed to write to the file"),
				)?;
				Ok::<_, tg::Error>(())
			};
			let result = tokio::runtime::Handle::current().block_on(future);
			match result {
				Ok(()) => {
					done = true;
				},
				Err(error_) => {
					error = Some(std::io::Error::other(error_));
				},
			}
		}
		if !done {
			return Err(tg::error!(?error, "failed to copy the file"));
		}

		// Get the dependencies' references.
		let dependencies = dependencies.keys().cloned().collect::<Vec<_>>();
		if !dependencies.is_empty() {
			let dependencies = serde_json::to_vec(&dependencies)
				.map_err(|source| tg::error!(!source, "failed to serialize dependencies"))?;
			xattr::set(dst, tg::file::XATTR_DEPENDENCIES_NAME, &dependencies)
				.map_err(|source| tg::error!(!source, "failed to write dependencies' xattr"))?;
		}

		// Set the file's permissions.
		if executable {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(path, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}
		Ok(())
	}

	fn checkout_inner_graph_symlink(
		&self,
		state: &mut State,
		path: PathBuf,
		_id: &tg::artifact::Id,
		graph: &tg::graph::Id,
		symlink: tg::graph::data::Symlink,
	) -> tg::Result<()> {
		// Render the target.
		let target = if let Some(artifact) = symlink.artifact {
			let mut target = PathBuf::new();

			// Get the id.
			let id = match artifact.clone() {
				Either::Left(node) => {
					let kind = state
						.graphs
						.get(graph)
						.unwrap()
						.nodes
						.get(node)
						.ok_or_else(|| tg::error!("expected the node to exist"))?
						.kind();
					let graph = graph.clone();
					let data: tg::artifact::Data = match kind {
						tg::artifact::Kind::Directory => {
							tg::directory::Data::Graph(tg::directory::data::Graph {
								graph: graph.clone(),
								node,
							})
							.into()
						},
						tg::artifact::Kind::File => tg::file::Data::Graph(tg::file::data::Graph {
							graph: graph.clone(),
							node,
						})
						.into(),
						tg::artifact::Kind::Symlink => {
							tg::symlink::Data::Graph(tg::symlink::data::Graph {
								graph: graph.clone(),
								node,
							})
							.into()
						},
					};
					let bytes = data.serialize()?;
					tg::artifact::Id::new(kind, &bytes)
				},
				Either::Right(id) => id,
			};

			if id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				// If the symlink's artifact is another artifact, then cache it and use the artifact's path.
				let artifact = artifact.clone().map_left(|node| (graph.clone(), node));
				self.checkout_dependency(state, &id, artifact)?;
				let artifacts_path = state
					.artifacts_path
					.as_ref()
					.ok_or_else(|| tg::error!("expected there to be an artifacts path"))?;
				target.push(artifacts_path.join(id.to_string()));
			}

			// Add the path if it is set.
			if let Some(path_) = symlink.path {
				target.push(path_);
			}

			// Diff the path.
			let src = path
				.parent()
				.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
			let dst = &target;
			crate::util::path::diff(src, dst)?
		} else if let Some(path_) = symlink.path {
			path_
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		std::os::unix::fs::symlink(target, path)
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		Ok(())
	}

	fn checkout_inner_data(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		data: tg::artifact::Data,
	) -> tg::Result<()> {
		match data {
			tg::artifact::Data::Directory(directory) => {
				self.checkout_inner_data_directory(state, path, id, directory)?;
			},
			tg::artifact::Data::File(file) => {
				self.checkout_inner_data_file(state, path, id, file)?;
			},
			tg::artifact::Data::Symlink(symlink) => {
				self.checkout_inner_data_symlink(state, path, id, symlink)?;
			},
		}
		Ok(())
	}

	fn checkout_inner_data_directory(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		directory: tg::directory::Data,
	) -> tg::Result<()> {
		let weight = directory.serialize().unwrap().len().to_u64().unwrap();
		let progress = state.progress.clone();
		match directory {
			tg::directory::Data::Graph(tg::directory::data::Graph { graph, node }) => {
				let artifact = Either::Left((graph, node));
				self.checkout_inner(state, path, id, artifact)?;
			},
			tg::directory::Data::Node(tg::directory::data::Node { entries }) => {
				std::fs::create_dir_all(&path).unwrap();
				for (name, id) in entries {
					let artifact = Either::Right(id.clone());
					let path = path.join(name);
					self.checkout_inner(state, path, &id, artifact)?;
				}
			},
		}
		progress.increment("bytes", weight);
		Ok(())
	}

	fn checkout_inner_data_file(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		file: tg::file::Data,
	) -> tg::Result<()> {
		let weight = file.serialize().unwrap().len().to_u64().unwrap();
		let progress = state.progress.clone();
		match file {
			tg::file::Data::Graph(data) => {
				let artifact = Either::Left((data.graph, data.node));
				self.checkout_inner(state, path, id, artifact)?;
			},
			tg::file::Data::Node(data) => {
				// Check out the dependencies.
				for referent in data.dependencies.values() {
					let Ok(id) = tg::artifact::Id::try_from(referent.item.clone()) else {
						continue;
					};
					if id != state.artifact {
						let artifact = Either::Right(id.clone());
						self.checkout_dependency(state, &id, artifact)?;
					}
				}

				// Copy the file.
				let src = &self.cache_path().join(id.to_string());
				let dst = &path;
				let mut done = false;
				let mut error = None;
				if !done {
					let size = std::fs::metadata(src)
						.ok()
						.map_or(0, |metadata| metadata.len());
					let result = reflink(src, dst);
					match result {
						Ok(()) => {
							done = true;
							state.progress.increment("bytes", size);
						},
						Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
							done = true;
							state.progress.increment("bytes", size);
						},
						Err(error_) => {
							error = Some(error_);
						},
					}
				}
				if !done {
					let server = self.clone();
					let path = path.clone();
					let progress = state.progress.clone();
					let future = async move {
						let reader = tg::Blob::with_id(data.contents)
							.read(&server, tg::blob::read::Arg::default())
							.await
							.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
						let mut reader = InspectReader::new(reader, {
							let progress = state.progress.clone();
							move |buf| {
								progress.increment("bytes", buf.len().to_u64().unwrap());
							}
						});
						let mut file = tokio::fs::File::create(&path).await.map_err(|source| {
							tg::error!(!source, ?path, "failed to create the file")
						})?;
						let size = tokio::io::copy(&mut reader, &mut file).await.map_err(
							|source| tg::error!(!source, ?path = path, "failed to write to the file"),
						)?;
						progress.increment("bytes", size);
						Ok::<_, tg::Error>(())
					};
					let result = tokio::runtime::Handle::current().block_on(future);
					match result {
						Ok(()) => {
							done = true;
						},
						Err(error_) => {
							error = Some(std::io::Error::other(error_));
						},
					}
				}
				if !done {
					return Err(tg::error!(?error, "failed to copy the file"));
				}

				// Get the dependencies' references.
				let dependencies = data.dependencies.keys().cloned().collect::<Vec<_>>();
				if !dependencies.is_empty() {
					let dependencies = serde_json::to_vec(&dependencies).map_err(|source| {
						tg::error!(!source, "failed to serialize dependencies")
					})?;
					xattr::set(dst, tg::file::XATTR_DEPENDENCIES_NAME, &dependencies).map_err(
						|source| tg::error!(!source, "failed to write dependencies' xattr"),
					)?;
				}

				// Set the file's permissions.
				if data.executable {
					let permissions = std::fs::Permissions::from_mode(0o755);
					std::fs::set_permissions(path, permissions)
						.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
				}
			},
		}
		progress.increment("bytes", weight);
		Ok(())
	}

	fn checkout_inner_data_symlink(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		symlink: tg::symlink::Data,
	) -> tg::Result<()> {
		let weight = symlink.serialize().unwrap().len().to_u64().unwrap();
		let progress = state.progress.clone();
		match symlink {
			tg::symlink::Data::Graph(data) => {
				let artifact = Either::Left((data.graph, data.node));
				self.checkout_inner(state, path, id, artifact)?;
			},
			tg::symlink::Data::Node(data) => {
				// Render the target.
				let target = if let Some(artifact) = &data.artifact {
					let mut target = PathBuf::new();

					if *artifact == state.artifact {
						// If the symlink's artifact is the root artifact, then use the root path.
						target.push(&state.path);
					} else {
						// If the symlink's artifact is another artifact, then check it out and use the artifact's path.
						self.checkout_dependency(state, artifact, Either::Right(artifact.clone()))?;
						let artifacts_path = state
							.artifacts_path
							.as_ref()
							.ok_or_else(|| tg::error!("expected there to be an artifacts path"))?;
						target.push(artifacts_path.join(artifact.to_string()));
					}

					// Add the path if it is set.
					if let Some(path_) = &data.path {
						target.push(path_);
					}

					// Diff the path.
					let src = path
						.parent()
						.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
					let dst = &target;
					crate::util::path::diff(src, dst)?
				} else if let Some(path_) = &data.path {
					path_.clone()
				} else {
					return Err(tg::error!("invalid symlink"));
				};

				// Create the symlink.
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
		}
		progress.increment("bytes", weight);
		Ok(())
	}

	fn checkout_write_lockfile(&self, id: tg::artifact::Id, state: &mut State) -> tg::Result<()> {
		// Create the lock.
		let lock = self
			.create_lockfile_for_artifact(&id, state.arg.dependencies)
			.map_err(|source| tg::error!(!source, "failed to create the lockfile"))?;

		// Do not write the lock if it is empty.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Write the lock.
		let artifact = tg::Artifact::with_id(id);
		if artifact.is_directory() {
			let contents = serde_json::to_vec_pretty(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lockfile"))?;
			let lockfile_path = state.path.join(tg::package::LOCKFILE_FILE_NAME);
			std::fs::write(&lockfile_path, &contents).map_err(
				|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write the lockfile"),
			)?;
		} else if artifact.is_file() {
			let contents = serde_json::to_vec(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lockfile"))?;
			xattr::set(&state.path, tg::file::XATTR_LOCK_NAME, &contents).map_err(|source| {
				tg::error!(!source, "failed to write the lockfile contents as an xattr")
			})?;
		}

		Ok(())
	}

	pub(crate) async fn handle_checkout_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.checkout(arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl std::ops::Add for Progress {
	type Output = Self;

	fn add(self, rhs: Self) -> Self::Output {
		Self::Output {
			objects: self.objects + rhs.objects,
			bytes: self.bytes + rhs.bytes,
		}
	}
}

impl std::ops::AddAssign for Progress {
	fn add_assign(&mut self, rhs: Self) {
		self.objects += rhs.objects;
		self.bytes += rhs.bytes;
	}
}

impl std::iter::Sum for Progress {
	fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
		iter.fold(Self::default(), |a, b| a + b)
	}
}
