use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	reflink_copy::reflink,
	std::{
		collections::HashSet,
		os::unix::fs::PermissionsExt as _,
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
		pin::pin,
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{Body, request::Ext as _},
	tangram_index::prelude::*,
	tangram_util::read::InspectReader,
};

mod lock;

struct State {
	arg: tg::checkout::Arg,
	artifact: tg::artifact::Id,
	artifacts_path: Option<PathBuf>,
	artifacts_path_created: bool,
	path: PathBuf,
	progress: crate::progress::Handle<tg::checkout::Output>,
	visited: HashSet<tg::artifact::Id, tg::id::BuildHasher>,
	visited_graphs: HashSet<tg::graph::Id, tg::id::BuildHasher>,
	visiting: HashSet<tg::artifact::Id, tg::id::BuildHasher>,
}

#[derive(Clone)]
pub struct Item {
	pub id: tg::artifact::Id,
	pub node: tg::graph::data::Node,
	pub graph: Option<tg::graph::Id>,
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
				let stream = self
					.cache(cache_arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to cache the artifact"))?;
				let context = context.clone();
				let extension = arg.extension.clone();
				let stream = stream
					.boxed()
					.map({
						let server = self.clone();
						move |result| {
							result.and_then(|event| match event {
								tg::progress::Event::Output(()) => {
									let path =
										server.artifacts_path().join(arg.artifact.to_string());

									// Add an extension if necessary.
									let path = if let Some(extension) = &extension {
										let path_with_extension = server
											.artifacts_path()
											.join(format!("{}{extension}", arg.artifact));
										std::fs::hard_link(&path, &path_with_extension).ok();
										path_with_extension
									} else {
										path
									};

									// Map the path if necessary.
									let path = if let Some(process) = &context.process {
										process.guest_path_for_host_path(path)?
									} else {
										path
									};

									let output =
										tg::progress::Event::Output(tg::checkout::Output { path });

									Ok(output)
								},
								event => Ok(event.map_output(|()| unreachable!())),
							})
						}
					})
					.left_stream()
					.left_stream();
				return Ok(stream);
			}

			let path = if let Some(ext) = &arg.extension {
				self.artifacts_path().join(format!("{}{ext}", arg.artifact))
			} else {
				path
			};

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
				// Ensure the artifact is stored.
				let result = server
					.checkout_ensure_stored(&artifact, &progress)
					.await
					.map_err(
						|source| tg::error!(!source, %artifact, "failed to ensure the artifact is stored"),
					);
				if let Err(error) = result {
					tracing::warn!(error = %error.trace());
					progress.log(
						Some(tg::progress::Level::Warning),
						"failed to ensure the artifact is stored".into(),
					);
				}

				progress.spinner("checkout", "checkout");
				progress.start(
					"artifacts".to_owned(),
					"artifacts".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					None,
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					None,
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

	pub(crate) async fn checkout_ensure_stored(
		&self,
		artifact: &tg::artifact::Id,
		progress: &crate::progress::Handle<tg::checkout::Output>,
	) -> tg::Result<()> {
		// Check if the artifact's subtree is stored.
		let stored = self
			.index
			.try_get_object(&artifact.clone().into())
			.await
			.map_err(
				|source| tg::error!(!source, %artifact, "failed to check if the artifact is stored"),
			)?
			.map(|object| object.stored)
			.unwrap_or_default();
		if stored.subtree {
			return Ok(());
		}

		// Index.
		let stream = self
			.index()
			.await
			.map_err(|source| tg::error!(!source, "failed to start the index"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the next index event"))?
		{
			progress.forward(Ok(event));
		}

		// Check if the artifact's subtree is stored.
		let stored = self
			.index
			.try_get_object(&artifact.clone().into())
			.await
			.map_err(
				|source| tg::error!(!source, %artifact, "failed to check if the artifact is stored"),
			)?
			.map(|object| object.stored)
			.unwrap_or_default();
		if stored.subtree {
			return Ok(());
		}

		// Pull.
		let stream = self
			.pull(tg::pull::Arg {
				items: vec![tg::Either::Left(artifact.clone().into())],
				..Default::default()
			})
			.await
			.map_err(|source| tg::error!(!source, %artifact, "failed to start the pull"))?;
		progress.spinner("pull", "pull");
		let mut stream = pin!(stream);
		while let Some(event) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the next pull event"))?
		{
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
					path,
					progress,
					visited: HashSet::default(),
					visited_graphs: HashSet::default(),
					visiting: HashSet::default(),
				};

				// Get the item.
				let edge = tg::graph::data::Edge::Object(state.artifact.clone());
				let item = server
					.checkout_get_item(edge)
					.map_err(|source| tg::error!(!source, "failed to get the item"))?;

				// Check out the artifact.
				let path = state.path.clone();
				server
					.checkout_artifact(&mut state, &path, &item)
					.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

				// Write the lock if necessary.
				server
					.checkout_write_lock(&mut state)
					.map_err(|source| tg::error!(!source, "failed to write the lock"))?;

				Ok::<_, tg::Error>(())
			}
		})
		.wait()
		.await
		.map_err(|source| tg::error!(!source, "the checkout task panicked"))?;

		// Remove the output if checkout failed.
		if let Err(error) = result {
			tangram_util::fs::remove(&path).await.ok();
			return Err(error);
		}

		let output = tg::checkout::Output { path };

		Ok(output)
	}

	fn checkout_dependency(&self, state: &mut State, item: &Item) -> tg::Result<()> {
		if !state.arg.dependencies {
			return Ok(());
		}

		// If the item is in a graph, then check out the graph.
		if let Some(graph_id) = &item.graph {
			self.checkout_graph(state, graph_id)?;
			return Ok(());
		}

		if !state.visited.insert(item.id.clone()) {
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
		let path = artifacts_path.join(item.id.to_string());

		// Save and clear the visiting set for the dependency checkout. Dependencies are checked out to a separate location and should have their own cycle detection.
		let visiting = std::mem::take(&mut state.visiting);
		let result = self.checkout_artifact(state, &path, item);
		state.visiting = visiting;

		result
	}

	fn checkout_graph(&self, state: &mut State, graph_id: &tg::graph::Id) -> tg::Result<()> {
		if !state.visited_graphs.insert(graph_id.clone()) {
			return Ok(());
		}

		// Load the graph data.
		let (_size, data) = self
			.store
			.try_get_object_data_sync(&graph_id.clone().into())
			.map_err(|source| tg::error!(!source, "failed to get the graph data"))?
			.ok_or_else(|| tg::error!("failed to load the graph"))?;
		let graph_data: tg::graph::Data = data
			.try_into()
			.map_err(|_| tg::error!("expected graph data"))?;

		// Get all items that need artifacts path entries.
		let items = Self::checkout_entry_items_for_graph(graph_id, &graph_data)?;

		// Ensure the artifacts path exists.
		let artifacts_path = state
			.artifacts_path
			.clone()
			.ok_or_else(|| tg::error!("cannot check out a dependency without an artifacts path"))?;
		if !state.artifacts_path_created {
			std::fs::create_dir_all(&artifacts_path).map_err(|source| {
				tg::error!(!source, "failed to create the artifacts directory")
			})?;
			state.artifacts_path_created = true;
		}

		// Check out all the items in the graph.
		for item in items {
			// Skip if already visited.
			if !state.visited.insert(item.id.clone()) {
				continue;
			}

			let path = artifacts_path.join(item.id.to_string());

			// Save and clear the visiting set for the dependency checkout.
			let visiting = std::mem::take(&mut state.visiting);
			let result = self.checkout_artifact(state, &path, &item);
			state.visiting = visiting;

			result?;
		}

		Ok(())
	}

	fn checkout_entry_items_for_graph(
		graph_id: &tg::graph::Id,
		graph_data: &tg::graph::Data,
	) -> tg::Result<Vec<Item>> {
		// Collect node indices which have incoming file dependency or symlink artifact edges in the graph.
		let mut marks = HashSet::<usize, fnv::FnvBuildHasher>::default();
		for node in &graph_data.nodes {
			match node {
				tg::graph::data::Node::File(file) => {
					for dependency in file.dependencies.values().flatten() {
						if let Some(tg::graph::data::Edge::Pointer(pointer)) = &dependency.item
							&& pointer.graph.is_none()
						{
							marks.insert(pointer.index);
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(tg::graph::data::Edge::Pointer(pointer)) = &symlink.artifact
						&& pointer.graph.is_none()
					{
						marks.insert(pointer.index);
					}
				},
				tg::graph::data::Node::Directory(_) => {},
			}
		}

		// Create items for nodes with incoming dependency edges.
		let mut items = Vec::new();
		for index in marks {
			let node = graph_data
				.nodes
				.get(index)
				.ok_or_else(|| tg::error!("invalid graph node index"))?
				.clone();

			let pointer = tg::graph::data::Pointer {
				graph: Some(graph_id.clone()),
				index,
				kind: node.kind(),
			};

			// Compute the artifact ID.
			let data: tg::artifact::data::Artifact = match node.kind() {
				tg::artifact::Kind::Directory => {
					tg::directory::Data::Pointer(pointer.clone()).into()
				},
				tg::artifact::Kind::File => tg::file::Data::Pointer(pointer.clone()).into(),
				tg::artifact::Kind::Symlink => tg::symlink::Data::Pointer(pointer.clone()).into(),
			};
			let bytes = data.serialize()?;
			let id = tg::artifact::Id::new(node.kind(), &bytes);

			items.push(Item {
				id,
				node,
				graph: Some(graph_id.clone()),
			});
		}

		Ok(items)
	}

	fn checkout_artifact(&self, state: &mut State, path: &Path, item: &Item) -> tg::Result<()> {
		// Checkout the artifact.
		match &item.node {
			tg::graph::data::Node::Directory(node) => {
				self.checkout_directory(state, path, item, node)?;
			},
			tg::graph::data::Node::File(node) => {
				self.checkout_file(state, path, item, node)?;
			},
			tg::graph::data::Node::Symlink(node) => {
				self.checkout_symlink(state, path, item, node)?;
			},
		}

		Ok(())
	}

	fn checkout_directory(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::Directory,
	) -> tg::Result<()> {
		let Item { id, graph, .. } = item;

		// Add to visiting set to detect cycles.
		state.visiting.insert(id.clone());

		// Create the directory.
		std::fs::create_dir_all(path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the directory"),
		)?;

		// Collect all entries, recursively flattening branches.
		let entries =
			crate::directory::collect_directory_entries(&self.store, node, graph.as_ref())?;

		// Recurse into the entries.
		for (name, mut edge) in entries {
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}
			let path = path.join(&name);
			let item = self
				.checkout_get_item(edge)
				.map_err(|source| tg::error!(!source, "failed to get the item"))?;

			// Check for a cycle.
			if state.visiting.contains(&item.id) {
				return Err(tg::error!("detected a directory cycle"));
			}

			self.checkout_artifact(state, &path, &item)
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;
		}

		// Remove from visiting set.
		state.visiting.remove(id);

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(())
	}

	fn checkout_file(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::File,
	) -> tg::Result<()> {
		let Item { id, graph, .. } = item;

		// Check out the dependencies.
		for dependency in node.dependencies.values() {
			let Some(dependency) = dependency else {
				continue;
			};
			let mut edge = match dependency.item.clone() {
				Some(tg::graph::data::Edge::Pointer(graph)) => {
					tg::graph::data::Edge::Pointer(graph)
				},
				Some(tg::graph::data::Edge::Object(id)) => match id.try_into() {
					Ok(id) => tg::graph::data::Edge::Object(id),
					Err(_) => continue,
				},
				None => continue,
			};
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}
			let item = self
				.checkout_get_item(edge)
				.map_err(|source| tg::error!(!source, "failed to get the item"))?;
			if item.id != state.artifact {
				self.checkout_dependency(state, &item)
					.map_err(|source| tg::error!(!source, "failed to check out the dependency"))?;
			}
		}

		let mut done = false;
		let contents = node
			.contents
			.as_ref()
			.ok_or_else(|| tg::error!("missing contents"))?;

		let src = &self.cache_path().join(id.to_string());
		let dst = path;

		// Attempt to reflink the file.
		let result = reflink(src, dst);
		if result.is_ok() {
			let len = std::fs::symlink_metadata(dst)
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
				.len();
			state.progress.increment("bytes", len);

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

				// Set the module xattr.
				if let Some(module) = &node.module {
					let module = module.to_string();
					xattr::set(path, tg::file::MODULE_XATTR_NAME, module.as_bytes()).map_err(
						|source| tg::error!(!source, "failed to write the module xattr"),
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
				}
			});
			let mut file = std::fs::File::create(path)
				.map_err(|source| tg::error!(!source, ?path, "failed to create the file"))?;
			std::io::copy(&mut reader, &mut file)
				.map_err(|source| tg::error!(!source, ?path, "failed to write to the file"))?;

			// Set the dependencies attr.
			let dependencies = node.dependencies.keys().cloned().collect::<Vec<_>>();
			if !dependencies.is_empty() {
				let dependencies = serde_json::to_vec(&dependencies).map_err(|source| {
					tg::error!(!source, "failed to serialize the dependencies")
				})?;
				xattr::set(path, tg::file::DEPENDENCIES_XATTR_NAME, &dependencies).map_err(
					|source| tg::error!(!source, "failed to write the dependencies attr"),
				)?;
			}

			// Set the module xattr.
			if let Some(module) = &node.module {
				let module = module.to_string();
				xattr::set(path, tg::file::MODULE_XATTR_NAME, module.as_bytes())
					.map_err(|source| tg::error!(!source, "failed to write the module xattr"))?;
			}

			// Set the permissions.
			if node.executable {
				let permissions = std::fs::Permissions::from_mode(0o755);
				std::fs::set_permissions(path, permissions)
					.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
			}
		}

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(())
	}

	fn checkout_symlink(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<()> {
		let Item { graph, .. } = item;

		// Render the target.
		let target = if let Some(mut edge) = node.artifact.clone() {
			let mut target = PathBuf::new();

			// Set the graph if necessary.
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}

			// Get the dependency node.
			let dependency_item = self
				.checkout_get_item(edge)
				.map_err(|source| tg::error!(!source, "failed to get the item"))?;

			if dependency_item.id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				// If the symlink's artifact is another artifact, then check it out and use the artifact's path.
				let dependency_id = dependency_item.id.clone();
				self.checkout_dependency(state, &dependency_item)?;

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
		state.progress.increment("artifacts", 1);

		Ok(())
	}

	fn checkout_get_item(&self, edge: tg::graph::data::Edge<tg::artifact::Id>) -> tg::Result<Item> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) => {
				// Load the graph.
				let graph_id = pointer
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?
					.clone();
				let (_size, data) = self
					.store
					.try_get_object_data_sync(&graph_id.clone().into())
					.map_err(|source| tg::error!(!source, "failed to get the graph data"))?
					.ok_or_else(|| tg::error!("failed to load the graph"))?;
				let graph_data: tg::graph::Data = data
					.try_into()
					.map_err(|_| tg::error!("expected graph data"))?;

				// Get the node.
				let node = graph_data
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid graph node"))?
					.clone();

				// Compute the id.
				let data: tg::artifact::data::Artifact = match node.kind() {
					tg::artifact::Kind::Directory => {
						tg::directory::Data::Pointer(pointer.clone()).into()
					},
					tg::artifact::Kind::File => tg::file::Data::Pointer(pointer.clone()).into(),
					tg::artifact::Kind::Symlink => {
						tg::symlink::Data::Pointer(pointer.clone()).into()
					},
				};
				let id = tg::artifact::Id::new(node.kind(), &data.serialize()?);

				Ok(Item {
					id,
					node,
					graph: Some(graph_id),
				})
			},

			tg::graph::data::Edge::Object(id) => {
				// Load the object.
				let (_size, data) = self
					.store
					.try_get_object_data_sync(&id.clone().into())
					.map_err(|source| tg::error!(!source, "failed to get the object data"))?
					.ok_or_else(|| tg::error!("failed to load the object"))?;
				let data = data
					.try_into()
					.map_err(|_| tg::error!("expected artifact data"))?;

				match data {
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Pointer(
						pointer,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Pointer(pointer))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Pointer(pointer)) => {
						// Load the graph.
						let graph_id = pointer
							.graph
							.as_ref()
							.ok_or_else(|| tg::error!("missing graph"))?
							.clone();
						let (_size, data) = self
							.store
							.try_get_object_data_sync(&graph_id.clone().into())
							.map_err(|source| tg::error!(!source, "failed to get the graph data"))?
							.ok_or_else(|| tg::error!("failed to load the graph"))?;
						let graph_data: tg::graph::Data = data
							.try_into()
							.map_err(|_| tg::error!("expected graph data"))?;

						// Get the node.
						let node = graph_data
							.nodes
							.get(pointer.index)
							.ok_or_else(|| tg::error!("invalid graph node"))?
							.clone();

						Ok(Item {
							id: id.clone(),
							node,
							graph: Some(graph_id),
						})
					},
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						Ok(Item {
							id: id.clone(),
							node: tg::graph::data::Node::Directory(node),
							graph: None,
						})
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => Ok(Item {
						id: id.clone(),
						node: tg::graph::data::Node::File(node),
						graph: None,
					}),
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						Ok(Item {
							id: id.clone(),
							node: tg::graph::data::Node::Symlink(node),
							graph: None,
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
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.checkout_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the checkout"))?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
