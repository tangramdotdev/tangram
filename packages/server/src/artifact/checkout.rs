use crate::Server;
use futures::{FutureExt as _, Stream, StreamExt as _};
use num::ToPrimitive;
use reflink_copy::reflink;
use std::{
	collections::{HashMap, HashSet},
	os::unix::fs::PermissionsExt as _,
	panic::AssertUnwindSafe,
	path::PathBuf,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tokio_util::{io::InspectReader, task::AbortOnDropHandle};

struct State {
	arg: tg::artifact::checkout::Arg,
	artifact: tg::artifact::Id,
	artifacts_path: Option<PathBuf>,
	artifacts_path_created: bool,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, fnv::FnvBuildHasher>,
	path: PathBuf,
	progress: crate::progress::Handle<tg::artifact::checkout::Output>,
	visited: HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug, Default)]
struct Progress {
	objects: u64,
	bytes: u64,
}

impl Server {
	pub async fn check_out_artifact(
		&self,
		artifact: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>
		+ Send
		+ 'static,
	> {
		let metadata = self
			.try_get_object_metadata(&artifact.clone().into())
			.await?;
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let artifact = artifact.clone();
			let arg = arg.clone();
			let progress = progress.clone();
			async move {
				// Ensure that the artifact is complete.
				if let Err(source) = server
					.ensure_artifact_is_complete(&artifact, &progress)
					.await
				{
					let error =
						tg::error!(!source, %artifact, "failed to pull or index the artifact");
					progress.error(error);
					return;
				};

				let title = if arg.path.is_none() {
					"checkout"
				} else {
					"cache"
				};
				progress.start(
					"checkout".to_owned(),
					title.to_owned(),
					tg::progress::IndicatorFormat::Normal,
					None,
					None,
				);

				let count = metadata.as_ref().and_then(|metadata| metadata.count);
				let weight = metadata.as_ref().and_then(|metadata| metadata.weight);
				progress.start(
					"checkout-objects".to_owned(),
					"objects".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					count,
				);
				progress.start(
					"checkout-bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					weight,
				);
				let result = AssertUnwindSafe(server.check_out_task(artifact, arg, &progress))
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
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	async fn check_out_task(
		&self,
		artifact: tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
		// Get the path.
		let Some(path) = arg.path.clone() else {
			if !self.vfs.lock().unwrap().is_some() {
				self.cache_artifact(&artifact, progress)
					.await
					.map_err(|source| tg::error!(!source, "failed to cache the artifact"))?;
			}
			let path = self.artifacts_path().join(artifact.to_string());
			let output = tg::artifact::checkout::Output { path };
			return Ok(output);
		};

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
				server.check_out_inner(&mut state, path, &id, artifact)?;

				// Write the lockfile if necessary.
				server.check_out_write_lockfile(id, &mut state)?;

				Ok::<_, tg::Error>(())
			}
		});
		let abort_handle = task.abort_handle();
		scopeguard::defer! {
			abort_handle.abort();
		}
		task.await.unwrap()?;

		let output = tg::artifact::checkout::Output { path };

		Ok(output)
	}

	fn check_out_dependency(
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
		self.check_out_inner(state, path, id, artifact)?;
		Ok(())
	}

	fn check_out_inner(
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
				self.check_out_inner_graph(state, path, id, &graph, node)?;
			},
			Either::Right(data) => {
				self.check_out_inner_data(state, path, id, data)?;
			},
		}
		state.progress.increment("objects", 1);
		Ok(())
	}

	fn check_out_inner_graph(
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
				self.check_out_inner_graph_directory(state, path, id, graph, &directory)?;
			},
			tg::graph::data::Node::File(file) => {
				self.check_out_inner_graph_file(state, path, id, graph, file)?;
			},
			tg::graph::data::Node::Symlink(symlink) => {
				self.check_out_inner_graph_symlink(state, path, id, graph, symlink)?;
			},
		}
		state.progress.increment("bytes", weight);
		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn check_out_inner_graph_directory(
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
						tg::artifact::Kind::Directory => tg::directory::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
						tg::artifact::Kind::File => tg::file::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
						tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
					};
					let bytes = data.serialize()?;
					tg::artifact::Id::new(kind, &bytes)
				},
				Either::Right(id) => id.clone(),
			};
			let artifact = artifact.clone().map_left(|node| (graph.clone(), node));
			let path = path.join(name);
			self.check_out_inner(state, path, &id, artifact)?;
		}
		Ok(())
	}

	fn check_out_inner_graph_file(
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
						tg::artifact::Kind::Directory => tg::directory::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
						tg::artifact::Kind::File => tg::file::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
						tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
							graph: graph.clone(),
							node,
						}
						.into(),
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
				self.check_out_dependency(state, &id, artifact)?;
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
				let _permit = server.file_descriptor_semaphore.acquire().await.unwrap();
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

		// Set the file's permissions.
		if executable {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(path, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}
		Ok(())
	}

	fn check_out_inner_graph_symlink(
		&self,
		state: &mut State,
		path: PathBuf,
		_id: &tg::artifact::Id,
		graph: &tg::graph::Id,
		symlink: tg::graph::data::Symlink,
	) -> tg::Result<()> {
		match symlink {
			tg::graph::data::Symlink::Target { target } => {
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
			tg::graph::data::Symlink::Artifact { artifact, subpath } => {
				// Check out the artifact.
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
							tg::artifact::Kind::Directory => tg::directory::Data::Graph {
								graph: graph.clone(),
								node,
							}
							.into(),
							tg::artifact::Kind::File => tg::file::Data::Graph {
								graph: graph.clone(),
								node,
							}
							.into(),
							tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
								graph: graph.clone(),
								node,
							}
							.into(),
						};
						let bytes = data.serialize()?;
						tg::artifact::Id::new(kind, &bytes)
					},
					Either::Right(id) => id.clone(),
				};
				if id != state.artifact {
					let artifact = artifact.clone().map_left(|node| (graph.clone(), node));
					self.check_out_dependency(state, &id, artifact)?;
				}

				// Render the target.
				let mut target = if id == state.artifact {
					// If the symlink's artifact is the root artifact, then use the root path.
					state.path.clone()
				} else {
					// Otherwise, use the artifact's path in the artifacts directory.
					let artifacts_path = state
						.artifacts_path
						.as_ref()
						.ok_or_else(|| tg::error!("expected there to be an artifacts path"))?;
					artifacts_path.join(artifact.to_string())
				};
				if let Some(subpath) = subpath {
					target = target.join(subpath);
				}
				let src = path
					.parent()
					.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
				let dst = &target;
				let target = crate::util::path::diff(src, dst)?;

				// Create the symlink.
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
		}
		Ok(())
	}

	fn check_out_inner_data(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		data: tg::artifact::Data,
	) -> tg::Result<()> {
		match data {
			tg::artifact::Data::Directory(directory) => {
				self.check_out_inner_data_directory(state, path, id, directory)?;
			},
			tg::artifact::Data::File(file) => {
				self.check_out_inner_data_file(state, path, id, file)?;
			},
			tg::artifact::Data::Symlink(symlink) => {
				self.check_out_inner_data_symlink(state, path, id, symlink)?;
			},
		}
		Ok(())
	}

	fn check_out_inner_data_directory(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		directory: tg::directory::Data,
	) -> tg::Result<()> {
		let weight = directory.serialize().unwrap().len().to_u64().unwrap();
		let progress = state.progress.clone();
		match directory {
			tg::directory::Data::Graph { graph, node } => {
				let artifact = Either::Left((graph, node));
				self.check_out_inner(state, path, id, artifact)?;
			},
			tg::directory::Data::Normal { entries } => {
				std::fs::create_dir_all(&path).unwrap();
				for (name, id) in entries {
					let artifact = Either::Right(id.clone());
					let path = path.join(name);
					self.check_out_inner(state, path, &id, artifact)?;
				}
			},
		}
		progress.increment("bytes", weight);
		Ok(())
	}

	fn check_out_inner_data_file(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		file: tg::file::Data,
	) -> tg::Result<()> {
		let weight = file.serialize().unwrap().len().to_u64().unwrap();
		let progress = state.progress.clone();
		match file {
			tg::file::Data::Graph { graph, node } => {
				let artifact = Either::Left((graph, node));
				self.check_out_inner(state, path, id, artifact)?;
			},
			tg::file::Data::Normal {
				contents,
				dependencies,
				executable,
			} => {
				// Check out the dependencies.
				for referent in dependencies.values() {
					let Ok(id) = tg::artifact::Id::try_from(referent.item.clone()) else {
						continue;
					};
					if id != state.artifact {
						let artifact = Either::Right(id.clone());
						self.check_out_dependency(state, &id, artifact)?;
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
						let _permit = server.file_descriptor_semaphore.acquire().await.unwrap();
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

				// Set the file's permissions.
				if executable {
					let permissions = std::fs::Permissions::from_mode(0o755);
					std::fs::set_permissions(path, permissions)
						.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
				}
			},
		}
		progress.increment("bytes", weight);
		Ok(())
	}

	fn check_out_inner_data_symlink(
		&self,
		state: &mut State,
		path: PathBuf,
		id: &tg::artifact::Id,
		symlink: tg::symlink::Data,
	) -> tg::Result<()> {
		let weight = symlink.serialize().unwrap().len().to_u64().unwrap();
		let progress = state.progress.clone();
		match symlink {
			tg::symlink::Data::Graph { graph, node } => {
				let artifact = Either::Left((graph, node));
				self.check_out_inner(state, path, id, artifact)?;
			},
			tg::symlink::Data::Target { target } => {
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
			tg::symlink::Data::Artifact { artifact, subpath } => {
				// Check out the artifact.
				if artifact != state.artifact {
					self.check_out_dependency(state, &artifact, Either::Right(artifact.clone()))?;
				}

				// Render the target.
				let mut target = if artifact == state.artifact {
					// If the symlink's artifact is the root artifact, then use the root path.
					state.path.clone()
				} else {
					// Otherwise, use the artifact's path in the artifacts directory.
					let artifacts_path = state
						.artifacts_path
						.as_ref()
						.ok_or_else(|| tg::error!("expected there to be an artifacts path"))?;
					artifacts_path.join(artifact.to_string())
				};
				if let Some(subpath) = subpath {
					target = target.join(subpath);
				}
				let src = path
					.parent()
					.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
				let dst = &target;
				let target = crate::util::path::diff(src, dst)?;

				// Create the symlink.
				std::os::unix::fs::symlink(target, path)
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			},
		}
		progress.increment("bytes", weight);
		Ok(())
	}

	fn check_out_write_lockfile(&self, id: tg::artifact::Id, state: &mut State) -> tg::Result<()> {
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
}

impl Server {
	pub(crate) async fn handle_check_out_artifact_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.check_out_artifact(&id, arg).await?;

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
