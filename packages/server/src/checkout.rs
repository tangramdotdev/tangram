use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	reflink_copy::reflink,
	std::{
		collections::{HashMap, HashSet},
		os::unix::fs::PermissionsExt as _,
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tangram_either::Either,
	tangram_futures::{
		stream::{Ext as _, TryExt as _},
		task::Task,
	},
	tangram_http::{Body, request::Ext as _},
	tokio_util::io::InspectReader,
};

mod lock;
mod progress;

struct State {
	arg: tg::checkout::Arg,
	artifact: tg::artifact::Id,
	artifacts_path: Option<PathBuf>,
	artifacts_path_created: bool,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>,
	lock_ids: Vec<tg::object::Id>,
	lock_nodes: Vec<Option<tg::graph::data::Node>>,
	lock_visited: HashMap<tg::artifact::Id, usize, tg::id::BuildHasher>,
	path: PathBuf,
	progress: crate::progress::Handle<tg::checkout::Output>,
	visited: HashSet<tg::artifact::Id, tg::id::BuildHasher>,
}

impl Server {
	pub(crate) async fn checkout_with_context(
		&self,
		context: &Context,
		mut arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + use<>,
	> {
		// If there is a process in the context, then replace the path with the host path.
		if let Some(process) = &context.process
			&& let Some(path) = &mut arg.path
		{
			*path = process.host_path_for_guest_path(path.clone());
		}

		// If the path is not provided, then cache.
		if arg.path.is_none() {
			let path = self.artifacts_path().join(arg.artifact.to_string());
			if self.vfs.lock().unwrap().is_none() {
				let cache_arg = tg::cache::Arg {
					artifacts: vec![arg.artifact.clone()],
				};
				let stream = self.cache(cache_arg).await?;
				let stream = stream
					.boxed()
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
					.left_stream();
				return Ok(stream);
			}
			let path = if let Some(process) = &context.process {
				process.guest_path_for_host_path(path.clone())?
			} else {
				path
			};
			let output = tg::checkout::Output { path };
			let event = tg::progress::Event::Output(output);
			let stream = stream::once(future::ok(event));
			let stream = stream.right_stream().left_stream();
			return Ok(stream);
		}

		let progress = crate::progress::Handle::new();
		let task = Task::spawn({
			let server = self.clone();
			let artifact = arg.artifact.clone();
			let arg = arg.clone();
			let progress = progress.clone();
			move |_| async move {
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
					.try_get_object_metadata(
						&arg.artifact.clone().into(),
						tg::object::metadata::Arg::default(),
					)
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

		let context = context.clone();
		let stream = progress
			.stream()
			.map_ok(move |event| {
				if let tg::progress::Event::Output(mut output) = event {
					if let Some(process) = &context.process {
						output.path = process.host_path_for_guest_path(output.path.clone());
					}
					tg::progress::Event::Output(output)
				} else {
					event
				}
			})
			.attach(task)
			.right_stream();

		Ok(stream)
	}

	pub(crate) async fn checkout_ensure_complete(
		&self,
		artifact: &tg::artifact::Id,
		progress: &crate::progress::Handle<tg::checkout::Output>,
	) -> tg::Result<()> {
		// Check if the artifact is complete.
		let complete = self
			.try_get_object_complete(&artifact.clone().into())
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
			.boxed()
		};

		// Create a future to index then check if the artifact is complete.
		let index_future = {
			let artifact = artifact.clone();
			let server = self.clone();
			async move {
				let stream = server.index().await?;
				let stream = std::pin::pin!(stream);
				stream.try_last().await?;
				let complete = server
					.try_get_object_complete(&artifact.clone().into())
					.await?
					.ok_or_else(|| tg::error!(%artifact, "expected an object"))?;
				if !complete {
					return Err(tg::error!("expected the object to be complete"));
				}
				Ok::<_, tg::Error>(())
			}
			.boxed()
		};

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
		let path = tangram_util::fs::canonicalize_parent(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path's parent"))?;

		// Determine the artifacts path.
		let artifacts_path: Option<PathBuf> = if artifact.is_directory() {
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

		// If an artifact exists, and this is a forced checkout, then return an error.
		if exists && arg.force {
			tangram_util::fs::remove(&path).await.ok();
		}

		// Checkout.
		let result = Task::spawn_blocking({
			let server = self.clone();
			let path = path.clone();
			let progress = progress.clone();
			move |_| {
				// Create the state.
				let mut state = State {
					arg,
					artifact,
					artifacts_path,
					artifacts_path_created: false,
					graphs: HashMap::default(),
					lock_ids: Vec::new(),
					lock_nodes: Vec::new(),
					lock_visited: HashMap::default(),
					path,
					progress,
					visited: HashSet::default(),
				};

				// Get the node.
				let id = state.artifact.clone();
				let edge = tg::graph::data::Edge::Object(id.clone());
				let (id, node, graph) = server.checkout_get_node(None, &edge)?;

				// Check out the artifact.
				let path = state.path.clone();
				server.checkout_inner(&mut state, &path, &id, node, graph.as_ref())?;

				// Write the lock if necessary.
				Self::checkout_write_lock(&mut state)?;

				Ok::<_, tg::Error>(())
			}
		})
		.wait()
		.await
		.unwrap();

		// Delete the partially constructed output if checkout failed.
		if let Err(error) = result {
			tangram_util::fs::remove(&path).await.ok();
			return Err(error);
		}

		let output = tg::checkout::Output { path };

		Ok(output)
	}

	fn checkout_dependency(
		&self,
		state: &mut State,
		id: &tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<usize> {
		// Check if we have already created a lock node for this artifact.
		if let Some(index) = state.lock_visited.get(id).copied() {
			return Ok(index);
		}

		if !state.arg.dependencies {
			// If dependencies are disabled, we still need to allocate a lock node.
			let index = state.lock_nodes.len();
			state.lock_visited.insert(id.clone(), index);
			state.lock_ids.push(id.clone().into());
			// Create a minimal lock node based on the node type.
			let lock_node = match node {
				tg::graph::data::Node::Directory(_) => {
					tg::graph::data::Node::Directory(tg::graph::data::Directory {
						entries: std::collections::BTreeMap::default(),
					})
				},
				tg::graph::data::Node::File(_) => {
					tg::graph::data::Node::File(tg::graph::data::File {
						contents: None,
						dependencies: std::collections::BTreeMap::default(),
						executable: false,
					})
				},
				tg::graph::data::Node::Symlink(node) => {
					tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
						artifact: None,
						path: node.path,
					})
				},
			};
			state.lock_nodes.push(Some(lock_node));
			return Ok(index);
		}

		if !state.visited.insert(id.clone()) {
			// Already visited for checkout, but should have lock node.
			return state
				.lock_visited
				.get(id)
				.copied()
				.ok_or_else(|| tg::error!("expected lock node to exist for visited artifact"));
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
		let index = self.checkout_inner(state, &path, id, node, graph)?;
		Ok(index)
	}

	fn checkout_inner(
		&self,
		state: &mut State,
		path: &Path,
		id: &tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<usize> {
		// Check if we have already created a lock node for this artifact.
		if let Some(index) = state.lock_visited.get(id).copied() {
			// We still need to perform the filesystem checkout at this path.
			// For symlinks and files, create them at the new path.
			// For directories, we need to recurse into entries.
			match &node {
				tg::graph::data::Node::Symlink(node) => {
					// Create the symlink at this path.
					let target = self.compute_symlink_target(state, path, id, graph, node)?;
					std::os::unix::fs::symlink(target, path)
						.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
				},
				_ => {
					// For files and directories, we don't duplicate them.
					// This is handled by the recursive calls in directory entries.
				},
			}
			return Ok(index);
		}

		// Allocate a lock node index.
		let index = state.lock_nodes.len();
		state.lock_visited.insert(id.clone(), index);
		state.lock_ids.push(id.clone().into());
		state.lock_nodes.push(None);

		// Checkout the artifact and create the lock node.
		let lock_node = match node {
			tg::graph::data::Node::Directory(node) => {
				self.checkout_inner_directory(state, path, id, graph, &node)?
			},
			tg::graph::data::Node::File(node) => {
				self.checkout_inner_file(state, path, id, graph, &node)?
			},
			tg::graph::data::Node::Symlink(node) => {
				self.checkout_inner_symlink(state, path, id, graph, &node)?
			},
		};

		// Store the lock node.
		state.lock_nodes[index].replace(lock_node);

		Ok(index)
	}

	fn compute_symlink_target(
		&self,
		state: &mut State,
		path: &Path,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<PathBuf> {
		let target = if let Some(edge) = &node.artifact {
			let mut target = PathBuf::new();

			// Set the graph if necessary.
			let mut edge = edge.clone();
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}

			// Get the dependency node.
			let (dependency_id, _dependency_node, _dependency_graph) =
				self.checkout_get_node(Some(&mut state.graphs), &edge)?;

			if dependency_id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				// If the symlink's artifact is another artifact, then use the artifact's path.
				let artifacts_path = state
					.artifacts_path
					.as_ref()
					.ok_or_else(|| tg::error!("expected there to be an artifacts path"))?;
				target.push(artifacts_path.join(dependency_id.to_string()));
			}

			// Add the path if it is set.
			if let Some(path) = &node.path {
				target.push(path);
			}

			// Diff the path.
			let src = path
				.parent()
				.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
			let dst = &target;
			tangram_util::path::diff(src, dst)
				.map_err(|source| tg::error!(!source, "failed to diff the paths"))?
		} else if let Some(path) = node.path.clone() {
			path
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		Ok(target)
	}

	fn checkout_get_node(
		&self,
		graphs: Option<&mut HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>>,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<(
		tg::artifact::Id,
		tg::graph::data::Node,
		Option<tg::graph::Id>,
	)> {
		let mut local_graphs = HashMap::default();
		let graphs = graphs.map_or(&mut local_graphs, |graphs| graphs);
		match edge {
			tg::graph::data::Edge::Reference(reference) => {
				// Load the graph.
				let graph = reference
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?;
				if !graphs.contains_key(graph) {
					let data = self
						.store
						.try_get_object_data_sync(&graph.clone().into())?
						.ok_or_else(|| tg::error!("failed to load the graph"))?
						.try_into()
						.map_err(|_| tg::error!("expected graph data"))?;
					graphs.insert(graph.clone(), data);
				}

				// Get the node.
				let node = graphs
					.get(graph)
					.unwrap()
					.nodes
					.get(reference.node)
					.ok_or_else(|| tg::error!("invalid graph node"))?
					.clone();

				// Compute the id.
				let data: tg::artifact::data::Artifact = match node.kind() {
					tg::artifact::Kind::Directory => {
						tg::directory::Data::Reference(reference.clone()).into()
					},
					tg::artifact::Kind::File => tg::file::Data::Reference(reference.clone()).into(),
					tg::artifact::Kind::Symlink => {
						tg::symlink::Data::Reference(reference.clone()).into()
					},
				};
				let id = tg::artifact::Id::new(node.kind(), &data.serialize()?);

				Ok((id, node, Some(graph.clone())))
			},

			tg::graph::data::Edge::Object(id) => {
				// Load the object.
				let data = self
					.store
					.try_get_object_data_sync(&id.clone().into())?
					.ok_or_else(|| tg::error!("failed to load the object"))?
					.try_into()
					.map_err(|_| tg::error!("expected artifact data"))?;

				match data {
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Reference(
						reference,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Reference(reference))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Reference(
						reference,
					)) => {
						// Load the graph.
						let graph = reference
							.graph
							.as_ref()
							.ok_or_else(|| tg::error!("missing graph"))?;
						if !graphs.contains_key(graph) {
							let data = self
								.store
								.try_get_object_data_sync(&graph.clone().into())?
								.ok_or_else(|| tg::error!("failed to load the graph"))?
								.try_into()
								.map_err(|_| tg::error!("expected graph data"))?;
							graphs.insert(graph.clone(), data);
						}

						// Get the node.
						let node = graphs
							.get(graph)
							.unwrap()
							.nodes
							.get(reference.node)
							.ok_or_else(|| tg::error!("invalid graph node"))?
							.clone();

						Ok((id.clone(), node, Some(graph.clone())))
					},
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						Ok((id.clone(), tg::graph::data::Node::Directory(node), None))
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						Ok((id.clone(), tg::graph::data::Node::File(node), None))
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						Ok((id.clone(), tg::graph::data::Node::Symlink(node), None))
					},
				}
			},
		}
	}

	fn checkout_inner_directory(
		&self,
		state: &mut State,
		path: &Path,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Directory,
	) -> tg::Result<tg::graph::data::Node> {
		// Create the directory.
		std::fs::create_dir_all(path).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to create the directory"),
		)?;

		// Recurse into the entries.
		let entries = node
			.entries
			.iter()
			.map(|(name, edge)| {
				let mut edge = edge.clone();
				if let tg::graph::data::Edge::Reference(reference) = &mut edge
					&& reference.graph.is_none()
				{
					reference.graph = graph.cloned();
				}
				let path = path.join(name);
				let (id, node, graph) = self.checkout_get_node(Some(&mut state.graphs), &edge)?;
				let node_index = self.checkout_inner(state, &path, &id, node, graph.as_ref())?;
				let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: None,
					node: node_index,
				});
				Ok::<_, tg::Error>((name.clone(), edge))
			})
			.collect::<tg::Result<_>>()?;

		let lock_node = tg::graph::data::Directory { entries };
		Ok(tg::graph::data::Node::Directory(lock_node))
	}

	fn checkout_inner_file(
		&self,
		state: &mut State,
		path: &Path,
		id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::File,
	) -> tg::Result<tg::graph::data::Node> {
		// Check out and create lock nodes for the dependencies.
		let lock_dependencies = node
			.dependencies
			.iter()
			.map(|(reference, referent)| {
				let Some(referent) = referent else {
					return Ok::<_, tg::Error>((reference.clone(), None));
				};
				let artifact = if state.arg.dependencies {
					match &referent.item {
						tg::graph::data::Edge::Reference(reference) => {
							let id = state.lock_ids[reference.node].clone().try_into().unwrap();
							Some(id)
						},
						tg::graph::data::Edge::Object(id) => {
							let id = id
								.clone()
								.try_into()
								.map_err(|_| tg::error!("expected an artifact"))?;
							Some(id)
						},
					}
				} else {
					None
				};
				let mut edge = match referent.item.clone() {
					tg::graph::data::Edge::Reference(reference) => {
						tg::graph::data::Edge::Reference(reference)
					},
					tg::graph::data::Edge::Object(id) => match id.try_into() {
						Ok(id) => tg::graph::data::Edge::Object(id),
						Err(_) => return Ok((reference.clone(), None)),
					},
				};
				if let tg::graph::data::Edge::Reference(reference) = &mut edge
					&& reference.graph.is_none()
				{
					reference.graph = graph.cloned();
				}
				let (id, node, graph) = self.checkout_get_node(Some(&mut state.graphs), &edge)?;
				let node_index = if id == state.artifact {
					// If this is the root artifact, we need to ensure a lock node exists for it.
					state.lock_visited.get(&id).copied().ok_or_else(|| {
						tg::error!("expected lock node to exist for root artifact")
					})?
				} else {
					self.checkout_dependency(state, &id, node, graph.as_ref())?
				};
				let item = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: None,
					node: node_index,
				});
				let options = tg::referent::Options {
					artifact,
					..referent.options.clone()
				};
				let referent = tg::Referent::new(item, options);
				Ok::<_, tg::Error>((reference.clone(), Some(referent)))
			})
			.collect::<tg::Result<_>>()?;

		let src = &self.cache_path().join(id.to_string());
		let dst = &path;

		// Attempt to reflink the file.
		let result = reflink(src, dst);
		if result.is_ok() {
			// Create the lock node for the file.
			let lock_node = tg::graph::data::File {
				contents: None,
				dependencies: lock_dependencies,
				executable: false,
			};
			return Ok(tg::graph::data::Node::File(lock_node));
		}

		// Attempt to write the file.
		let result = tokio::runtime::Handle::current().block_on({
			let server = self.clone();
			async move {
				let dst = &dst;
				let contents = node
					.contents
					.as_ref()
					.ok_or_else(|| tg::error!("missing contents"))?;
				let mut reader = tg::Blob::with_id(contents.clone())
					.read(&server, tg::read::Options::default())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
				let mut reader = InspectReader::new(&mut reader, {
					let progress = state.progress.clone();
					move |buf| {
						progress.increment("bytes", buf.len().to_u64().unwrap());
					}
				});
				let mut file = tokio::fs::File::create(dst).await.map_err(
					|source| tg::error!(!source, ?path = dst, "failed to create the file"),
				)?;
				tokio::io::copy(&mut reader, &mut file).await.map_err(
					|source| tg::error!(!source, ?path = dst, "failed to write to the file"),
				)?;
				Ok::<_, tg::Error>(())
			}
		});
		if let Err(error) = result {
			return Err(tg::error!(?error, "failed to copy the file"));
		}

		// Set the dependencies attr.
		let dependencies = node.dependencies.keys().cloned().collect::<Vec<_>>();
		if !dependencies.is_empty() {
			let dependencies = serde_json::to_vec(&dependencies)
				.map_err(|source| tg::error!(!source, "failed to serialize the dependencies"))?;
			xattr::set(dst, tg::file::DEPENDENCIES_XATTR_NAME, &dependencies)
				.map_err(|source| tg::error!(!source, "failed to write the dependencies attr"))?;
		}

		// Set the permissions.
		if node.executable {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(dst, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		// Create the lock node for the file.
		let lock_node = tg::graph::data::File {
			contents: None,
			dependencies: lock_dependencies,
			executable: false,
		};
		Ok(tg::graph::data::Node::File(lock_node))
	}

	fn checkout_inner_symlink(
		&self,
		state: &mut State,
		path: &Path,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<tg::graph::data::Node> {
		// Process the artifact edge and create lock node.
		let lock_artifact = node
			.artifact
			.as_ref()
			.map(|edge| {
				let mut edge = edge.clone();
				if let tg::graph::data::Edge::Reference(reference) = &mut edge
					&& reference.graph.is_none()
				{
					reference.graph = graph.cloned();
				}
				let (dependency_id, dependency_node, dependency_graph) =
					self.checkout_get_node(Some(&mut state.graphs), &edge)?;
				let node_index = if dependency_id == state.artifact {
					// If the symlink's artifact is the root artifact, we need to ensure a lock node exists for it.
					state
						.lock_visited
						.get(&dependency_id)
						.copied()
						.ok_or_else(|| {
							tg::error!("expected lock node to exist for root artifact")
						})?
				} else {
					// If the symlink's artifact is another artifact, then check it out.
					self.checkout_dependency(
						state,
						&dependency_id,
						dependency_node,
						dependency_graph.as_ref(),
					)?
				};
				Ok::<_, tg::Error>(tg::graph::data::Edge::Reference(
					tg::graph::data::Reference {
						graph: None,
						node: node_index,
					},
				))
			})
			.transpose()?;

		// Render the target.
		let target = if let Some(edge) = &node.artifact {
			let mut target = PathBuf::new();

			// Set the graph if necessary.
			let mut edge = edge.clone();
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}

			// Get the dependency node.
			let (dependency_id, _dependency_node, _dependency_graph) =
				self.checkout_get_node(Some(&mut state.graphs), &edge)?;

			if dependency_id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				// If the symlink's artifact is another artifact, then use the artifact's path.
				let artifacts_path = state
					.artifacts_path
					.as_ref()
					.ok_or_else(|| tg::error!("expected there to be an artifacts path"))?;
				target.push(artifacts_path.join(dependency_id.to_string()));
			}

			// Add the path if it is set.
			if let Some(path) = &node.path {
				target.push(path);
			}

			// Diff the path.
			let src = path
				.parent()
				.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
			let dst = &target;
			tangram_util::path::diff(src, dst)
				.map_err(|source| tg::error!(!source, "failed to diff the paths"))?
		} else if let Some(path) = node.path.clone() {
			path
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		std::os::unix::fs::symlink(target, path)
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		// Create the lock node for the symlink.
		let lock_node = tg::graph::data::Symlink {
			artifact: lock_artifact,
			path: node.path.clone(),
		};
		Ok(tg::graph::data::Node::Symlink(lock_node))
	}

	pub(crate) async fn handle_checkout_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = self.checkout_with_context(context, arg).await?;

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
