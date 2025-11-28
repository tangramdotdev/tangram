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
	tangram_util::read::InspectReader,
	tokio_stream::wrappers::UnboundedReceiverStream,
};

mod lock;
mod progress;

struct State {
	arg: tg::checkout::Arg,
	artifact: tg::artifact::Id,
	artifacts_path: Option<PathBuf>,
	artifacts_path_created: bool,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>,
	path: PathBuf,
	progress: crate::progress::Handle<tg::checkout::Output>,
	progress_task_sender: tokio::sync::mpsc::UnboundedSender<ProgressTaskMessage>,
	visited: HashSet<tg::artifact::Id, tg::id::BuildHasher>,
}

struct GetNodeOutput {
	id: tg::artifact::Id,
	node: tg::graph::data::Node,
	graph: Option<tg::graph::Id>,
	size: Option<u64>,
}

struct ProgressTaskMessage {
	id: tg::object::Id,
	count: u64,
	weight: u64,
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

		// Index.
		let stream = self.index().await?;
		let stream = std::pin::pin!(stream);
		stream.try_last().await?;

		// Check if the artifact is complete.
		let complete = self
			.try_get_object_complete(&artifact.clone().into())
			.await?
			.unwrap_or_default();
		if complete {
			return Ok(());
		}

		// Pull.
		let stream = self
			.pull(tg::pull::Arg {
				items: vec![Either::Left(artifact.clone().into())],
				remote: Some("default".to_owned()),
				..Default::default()
			})
			.await?;
		progress.spinner("pull", "pull");
		let mut stream = std::pin::pin!(stream);
		while let Some(event) = stream.try_next().await? {
			progress.forward(Ok(event));
		}

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
		if !path.is_absolute() {
			return Err(tg::error!(?path, "the path must be absolute"));
		}
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

		// Create a channel for the progress task.
		let (progress_task_sender, progress_task_receiver) = tokio::sync::mpsc::unbounded_channel();

		// Spawn the progress task.
		let progress_task = Task::spawn({
			let server = self.clone();
			let progress = progress.clone();
			|_| async move {
				server
					.checkout_progress_task(&progress, progress_task_receiver)
					.await;
			}
		});

		// Checkout.
		let result = Task::spawn_blocking({
			let server = self.clone();
			let path = path.clone();
			let progress = progress.clone();
			let progress_task_sender = progress_task_sender.clone();
			move |_| {
				// Create the state.
				let mut state = State {
					arg,
					artifact,
					artifacts_path,
					artifacts_path_created: false,
					graphs: HashMap::default(),
					path,
					progress,
					progress_task_sender,
					visited: HashSet::default(),
				};

				// Get the node.
				let id = state.artifact.clone();
				let edge = tg::graph::data::Edge::Object(id.clone());
				let GetNodeOutput {
					id,
					node,
					graph,
					size,
				} = server.checkout_get_node(None, &edge, &state.progress)?;

				// Check out the artifact.
				let path = state.path.clone();
				server.checkout_artifact(
					&mut state,
					&path,
					&edge,
					&id,
					node,
					graph.as_ref(),
					size,
				)?;

				// Write the lock if necessary.
				server.checkout_write_lock(&mut state)?;

				Ok::<_, tg::Error>(())
			}
		})
		.wait()
		.await
		.map_err(|source| tg::error!(!source, "the checkout task panicked"))?;

		// Abort the progress task.
		progress_task.abort();

		// Delete the partially constructed output if checkout failed.
		if let Err(error) = result {
			tangram_util::fs::remove(&path).await.ok();
			return Err(error);
		}

		let output = tg::checkout::Output { path };

		Ok(output)
	}

	async fn checkout_progress_task(
		&self,
		progress: &crate::progress::Handle<tg::checkout::Output>,
		progress_task_receiver: tokio::sync::mpsc::UnboundedReceiver<ProgressTaskMessage>,
	) {
		let mut stream = UnboundedReceiverStream::new(progress_task_receiver).ready_chunks(256);
		while let Some(messages) = stream.next().await {
			let ids = messages
				.iter()
				.map(|message| message.id.clone())
				.collect::<Vec<_>>();
			let Ok(metadata) = self.try_get_object_complete_and_metadata_batch(&ids).await else {
				continue;
			};
			for (message, metadata) in std::iter::zip(messages, metadata) {
				if let Some((_, metadata)) = metadata {
					if let Some(count) = metadata.count {
						progress.increment("objects", count.saturating_sub(message.count));
					}
					if let Some(weight) = metadata.weight {
						progress.increment("bytes", weight.saturating_sub(message.weight));
					}
				}
			}
		}
	}

	fn checkout_dependency(
		&self,
		state: &mut State,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		id: &tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<&tg::graph::Id>,
		size: Option<u64>,
	) -> tg::Result<()> {
		if !state.arg.dependencies {
			return Ok(());
		}
		if !state.visited.insert(id.clone()) {
			if let tg::graph::data::Edge::Object(id) = edge {
				let message = ProgressTaskMessage {
					id: id.clone().into(),
					count: 0,
					weight: 0,
				};
				state.progress_task_sender.send(message).ok();
			}
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
		self.checkout_artifact(state, &path, edge, id, node, graph, size)?;
		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	fn checkout_artifact(
		&self,
		state: &mut State,
		path: &Path,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		id: &tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<&tg::graph::Id>,
		size: Option<u64>,
	) -> tg::Result<()> {
		// Checkout the artifact.
		match node {
			tg::graph::data::Node::Directory(node) => {
				self.checkout_directory(state, path, edge, id, graph, &node, size)?;
			},
			tg::graph::data::Node::File(node) => {
				self.checkout_file(state, path, edge, id, graph, &node, size)?;
			},
			tg::graph::data::Node::Symlink(node) => {
				self.checkout_symlink(state, path, edge, id, graph, &node, size)?;
			},
		}

		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	fn checkout_directory(
		&self,
		state: &mut State,
		path: &Path,
		_edge: &tg::graph::data::Edge<tg::artifact::Id>,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Directory,
		size: Option<u64>,
	) -> tg::Result<()> {
		// Create the directory.
		std::fs::create_dir_all(path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the directory"),
		)?;

		// Recurse into the entries.
		for (name, edge) in &node.entries {
			let mut edge = edge.clone();
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}
			let path = path.join(name);
			let GetNodeOutput {
				id,
				node,
				graph,
				size,
			} = self.checkout_get_node(Some(&mut state.graphs), &edge, &state.progress)?;
			self.checkout_artifact(state, &path, &edge, &id, node, graph.as_ref(), size)?;
		}

		// Increment the progress.
		if let Some(size) = size {
			state.progress.increment("objects", 1);
			state.progress.increment("bytes", size);
		}

		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	fn checkout_file(
		&self,
		state: &mut State,
		path: &Path,
		_edge: &tg::graph::data::Edge<tg::artifact::Id>,
		id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::File,
		size: Option<u64>,
	) -> tg::Result<()> {
		// Check out the dependencies.
		for referent in node.dependencies.values() {
			let Some(referent) = referent else {
				continue;
			};
			let mut edge = match referent.item.clone() {
				tg::graph::data::Edge::Reference(graph) => tg::graph::data::Edge::Reference(graph),
				tg::graph::data::Edge::Object(id) => match id.try_into() {
					Ok(id) => tg::graph::data::Edge::Object(id),
					Err(_) => continue,
				},
			};
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}
			let GetNodeOutput {
				id,
				node,
				graph,
				size,
			} = self.checkout_get_node(Some(&mut state.graphs), &edge, &state.progress)?;
			if id != state.artifact {
				self.checkout_dependency(state, &edge, &id, node, graph.as_ref(), size)?;
			}
		}

		let mut done = false;
		let contents = node
			.contents
			.as_ref()
			.ok_or_else(|| tg::error!("missing contents"))?;
		let mut weight = 0;

		let src = &self.cache_path().join(id.to_string());
		let dst = &path;

		// Attempt to reflink the file.
		let result = reflink(src, dst);
		if result.is_ok() {
			let len = std::fs::symlink_metadata(dst)
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
				.len();
			state.progress.increment("bytes", len);
			weight += len;

			if cfg!(target_os = "linux") {
				// Set the dependencies attr.
				let dependencies = node.dependencies.keys().cloned().collect::<Vec<_>>();
				if !dependencies.is_empty() {
					let dependencies = serde_json::to_vec(&dependencies).map_err(|source| {
						tg::error!(!source, "failed to serialize the dependencies")
					})?;
					xattr::set(dst, tg::file::DEPENDENCIES_XATTR_NAME, &dependencies).map_err(
						|source| tg::error!(!source, "failed to write the dependencies attr"),
					)?;
				}

				// Set the permissions.
				if node.executable {
					let permissions = std::fs::Permissions::from_mode(0o755);
					std::fs::set_permissions(dst, permissions)
						.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
				}
			}

			done = true;
		}

		// Otherwise, write the file.
		if !done {
			let mut reader =
				crate::read::Reader::new_sync(self, tg::Blob::with_id(contents.clone()))
					.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
			let mut reader = InspectReader::new(&mut reader, {
				|buffer| {
					let len = buffer.len().to_u64().unwrap();
					state.progress.increment("bytes", len);
					weight += len;
				}
			});
			let mut file = std::fs::File::create(dst)
				.map_err(|source| tg::error!(!source, path = ?dst, "failed to create the file"))?;
			std::io::copy(&mut reader, &mut file).map_err(
				|source| tg::error!(!source, path = ?dst, "failed to write to the file"),
			)?;

			// Set the dependencies attr.
			let dependencies = node.dependencies.keys().cloned().collect::<Vec<_>>();
			if !dependencies.is_empty() {
				let dependencies = serde_json::to_vec(&dependencies).map_err(|source| {
					tg::error!(!source, "failed to serialize the dependencies")
				})?;
				xattr::set(dst, tg::file::DEPENDENCIES_XATTR_NAME, &dependencies).map_err(
					|source| tg::error!(!source, "failed to write the dependencies attr"),
				)?;
			}

			// Set the permissions.
			if node.executable {
				let permissions = std::fs::Permissions::from_mode(0o755);
				std::fs::set_permissions(dst, permissions)
					.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
			}
		}

		// Increment the progress.
		if let Some(size) = size {
			state.progress.increment("objects", 1);
			state.progress.increment("bytes", size);
		}

		// Send the progress task message for the contents.
		let message = ProgressTaskMessage {
			id: contents.clone().into(),
			count: 0,
			weight,
		};
		state.progress_task_sender.send(message).ok();

		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	fn checkout_symlink(
		&self,
		state: &mut State,
		path: &Path,
		_edge: &tg::graph::data::Edge<tg::artifact::Id>,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Symlink,
		size: Option<u64>,
	) -> tg::Result<()> {
		// Render the target.
		let target = if let Some(mut edge) = node.artifact.clone() {
			let mut target = PathBuf::new();

			// Set the graph if necessary.
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}

			// Get the dependency node.
			let GetNodeOutput {
				id: dependency_id,
				node: dependency_node,
				graph: dependency_graph,
				size: dependency_size,
			} = self.checkout_get_node(Some(&mut state.graphs), &edge, &state.progress)?;

			if dependency_id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				// If the symlink's artifact is another artifact, then check it out and use the artifact's path.
				self.checkout_dependency(
					state,
					&edge,
					&dependency_id,
					dependency_node,
					dependency_graph.as_ref(),
					dependency_size,
				)?;

				// Update the target.
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

		// Increment the progress.
		if let Some(size) = size {
			state.progress.increment("objects", 1);
			state.progress.increment("bytes", size);
		}

		Ok(())
	}

	fn checkout_get_node(
		&self,
		graphs: Option<&mut HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>>,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		progress: &crate::progress::Handle<tg::checkout::Output>,
	) -> tg::Result<GetNodeOutput> {
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
					let (size, data) = self
						.store
						.try_get_object_data_sync(&graph.clone().into())?
						.ok_or_else(|| tg::error!("failed to load the graph"))?;
					let data = data
						.try_into()
						.map_err(|_| tg::error!("expected graph data"))?;
					graphs.insert(graph.clone(), data);

					// Increment progress for the graph.
					progress.increment("objects", 1);
					progress.increment("bytes", size);
				}

				// Get the node.
				let node = graphs
					.get(graph)
					.unwrap()
					.nodes
					.get(reference.index)
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

				Ok(GetNodeOutput {
					id,
					node,
					graph: Some(graph.clone()),
					size: None,
				})
			},

			tg::graph::data::Edge::Object(id) => {
				// Load the object.
				let (size, data) = self
					.store
					.try_get_object_data_sync(&id.clone().into())?
					.ok_or_else(|| tg::error!("failed to load the object"))?;
				let data = data
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
							let (size, data) = self
								.store
								.try_get_object_data_sync(&graph.clone().into())?
								.ok_or_else(|| tg::error!("failed to load the graph"))?;
							let data = data
								.try_into()
								.map_err(|_| tg::error!("expected graph data"))?;
							graphs.insert(graph.clone(), data);

							// Increment progress for the graph.
							progress.increment("objects", 1);
							progress.increment("bytes", size);
						}

						// Get the node.
						let node = graphs
							.get(graph)
							.unwrap()
							.nodes
							.get(reference.index)
							.ok_or_else(|| tg::error!("invalid graph node"))?
							.clone();

						Ok(GetNodeOutput {
							id: id.clone(),
							node,
							graph: Some(graph.clone()),
							size: Some(size),
						})
					},
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						Ok(GetNodeOutput {
							id: id.clone(),
							node: tg::graph::data::Node::Directory(node),
							graph: None,
							size: Some(size),
						})
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						Ok(GetNodeOutput {
							id: id.clone(),
							node: tg::graph::data::Node::File(node),
							graph: None,
							size: Some(size),
						})
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						Ok(GetNodeOutput {
							id: id.clone(),
							node: tg::graph::data::Node::Symlink(node),
							graph: None,
							size: Some(size),
						})
					},
				}
			},
		}
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
