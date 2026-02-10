use {
	crate::{Context, Server, temp::Temp},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	itertools::Itertools as _,
	num::ToPrimitive as _,
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

struct State {
	artifact: tg::artifact::Id,
	path: PathBuf,
	progress: crate::progress::Handle<()>,
	visiting: HashSet<tg::artifact::Id, tg::id::BuildHasher>,
}

#[derive(Clone)]
pub struct Item {
	pub id: tg::artifact::Id,
	pub node: tg::graph::data::Node,
	pub graph: Option<tg::graph::Id>,
}

impl Server {
	pub(crate) async fn cache_with_context(
		&self,
		_context: &Context,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<>> {
		let tg::cache::Arg { artifacts } = arg;
		if artifacts.is_empty() {
			return Ok(stream::once(future::ok(tg::progress::Event::Output(()))).left_stream());
		}
		let progress = crate::progress::Handle::new();
		let task = Task::spawn({
			let server = self.clone();
			let progress = progress.clone();
			|_| async move {
				// Ensure the artifact is stored.
				let result = server
					.cache_ensure_stored(&artifacts, &progress)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							?artifacts,
							"failed to ensure the artifacts are stored"
						)
					});
				if let Err(error) = result {
					tracing::warn!(error = %error.trace());
					progress.log(
						Some(tg::progress::Level::Warning),
						"failed to ensure the artifacts are stored".into(),
					);
				}

				// Create the progress indicators.
				progress.spinner("cache", "cache");
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

				let result = future::try_join_all(artifacts.into_iter().map({
					|artifact| {
						let server = server.clone();
						let progress = progress.clone();
						async move {
							AssertUnwindSafe(server.cache_task(&artifact, &progress))
								.catch_unwind()
								.await
						}
					}
				}))
				.await
				.map(|results| results.into_iter().try_collect::<_, (), _>());

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
		let stream = progress.stream().attach(task).right_stream();
		Ok(stream)
	}

	async fn cache_ensure_stored(
		&self,
		artifacts: &[tg::artifact::Id],
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Check if the artifacts' subtrees are stored.
		let ids = artifacts
			.iter()
			.map(|id| id.clone().into())
			.collect::<Vec<_>>();
		let stored = self
			.index
			.try_get_objects(&ids)
			.await?
			.iter()
			.all(|object| object.as_ref().is_some_and(|object| object.stored.subtree));
		if stored {
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

		// Check if the artifacts' subtrees are stored.
		let ids = artifacts
			.iter()
			.map(|id| id.clone().into())
			.collect::<Vec<_>>();
		let stored = self
			.index
			.try_get_objects(&ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the artifacts are stored"))?
			.iter()
			.all(|object| object.as_ref().is_some_and(|object| object.stored.subtree));
		if stored {
			return Ok(());
		}

		// Pull.
		let stream = self
			.pull(tg::pull::Arg {
				items: artifacts
					.iter()
					.map(|artifact| tg::Either::Left(artifact.clone().into()))
					.collect(),
				..Default::default()
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to start the pull"))?;
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

	async fn cache_task(
		&self,
		id: &tg::artifact::Id,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Get the item in a blocking task.
		let edge = tg::graph::data::Edge::Object(id.clone());
		let item = tokio::task::spawn_blocking({
			let server = self.clone();
			move || server.cache_get_item(edge)
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		// Cache the entry and all its dependencies.
		self.cache_artifact(item, progress.clone()).await
	}

	fn cache_artifact(
		&self,
		item: Item,
		progress: crate::progress::Handle<()>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let server = self.clone();
		async move {
			let task = server.cache_tasks.get_or_spawn_with_context(
				item.id.clone(),
				|| {
					let progress = crate::progress::Handle::new();
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
					progress
				},
				{
					let server = server.clone();
					move |dependency_progress, _| {
						let server = server.clone();
						let item = item.clone();
						let dependency_progress = dependency_progress.clone();
						async move { server.cache_artifact_task(item, dependency_progress).await }
					}
				},
			);

			// Forward progress events from the dependency to the progress handle.
			let mut dependency_artifacts = 0u64;
			let mut dependency_bytes = 0u64;
			let mut stream = pin!(task.context().stream().fuse());
			let mut task_future = pin!(task.wait().fuse());
			loop {
				futures::select! {
					event = stream.next() => {
						if let Some(Ok(tg::progress::Event::Indicators(indicators))) = event {
							for indicator in indicators {
								if indicator.name == "artifacts" && let Some(current) = indicator.current {
									progress.increment("artifacts", current.saturating_sub(dependency_artifacts));
									dependency_artifacts = current;
								} else if indicator.name == "bytes" && let Some(current) = indicator.current {
									progress.increment("bytes", current.saturating_sub(dependency_bytes));
									dependency_bytes = current;
								}
							}
						}
					}
					result = task_future => {
						return result
							.map_err(|source| tg::error!(!source, "a cache task panicked"))
							.and_then(|result| result);
					}
				}
			}
		}
	}

	fn cache_graph(
		&self,
		graph_id: &tg::graph::Id,
		progress: crate::progress::Handle<()>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let server = self.clone();
		let graph_id = graph_id.clone();
		async move {
			let task = server.cache_graph_tasks.get_or_spawn_with_context(
				graph_id.clone(),
				|| {
					let progress = crate::progress::Handle::new();
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
					progress
				},
				{
					let server = server.clone();
					move |dependency_progress, _| {
						let server = server.clone();
						let graph_id = graph_id.clone();
						let dependency_progress = dependency_progress.clone();
						async move {
							server
								.cache_graph_task(&graph_id, dependency_progress)
								.await
						}
					}
				},
			);

			// Forward progress events from the dependency to the progress handle.
			let mut dependency_artifacts = 0u64;
			let mut dependency_bytes = 0u64;
			let mut stream = pin!(task.context().stream().fuse());
			let mut task_future = pin!(task.wait().fuse());
			loop {
				futures::select! {
					event = stream.next() => {
						if let Some(Ok(tg::progress::Event::Indicators(indicators))) = event {
							for indicator in indicators {
								if indicator.name == "artifacts" && let Some(current) = indicator.current {
									progress.increment("artifacts", current.saturating_sub(dependency_artifacts));
									dependency_artifacts = current;
								} else if indicator.name == "bytes" && let Some(current) = indicator.current {
									progress.increment("bytes", current.saturating_sub(dependency_bytes));
									dependency_bytes = current;
								}
							}
						}
					}
					result = task_future => {
						return result
							.map_err(|source| tg::error!(!source, "a cache graph task panicked"))
							.and_then(|result| result);
					}
				}
			}
		}
	}

	async fn cache_artifact_task(
		&self,
		item: Item,
		progress: crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// If this item is in a graph, ensure the graph's cycle-related items are cached first.
		if let Some(graph_id) = &item.graph {
			self.cache_graph(graph_id, progress.clone())
				.await
				.map_err(|source| tg::error!(!source, %graph_id, "failed to cache the graph"))?;
		}

		// Create the path.
		let path = self.cache_path().join(item.id.to_string());

		// If the path exists, then return.
		let exists = tokio::task::spawn_blocking({
			let path = path.clone();
			move || path.try_exists()
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
		.map_err(|source| tg::error!(!source, "failed to determine if the path exists"))?;
		if exists {
			return Ok(());
		}

		// Create the temp and write the artifact.
		let (temp, dependencies) = tokio::task::spawn_blocking({
			let server = self.clone();
			let item = item.clone();
			let progress = progress.clone();
			move || {
				let temp = Temp::new(&server);
				let dependencies = server.cache_write(temp.path(), &item, &progress)?;
				Ok::<_, tg::Error>((temp, dependencies))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;

		let dependency_ids: Vec<tg::artifact::Id> = dependencies
			.iter()
			.map(|dependency| dependency.id.clone())
			.collect();

		// Await the dependency cache tasks.
		future::try_join_all(
			dependencies
				.into_iter()
				.map(|dependency| self.cache_artifact(dependency, progress.clone())),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to cache the dependencies"))?;

		// Rename the temp to the cache directory.
		tokio::task::spawn_blocking({
			let server = self.clone();
			move || server.cache_rename(item, &temp, &dependency_ids)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;

		Ok(())
	}

	async fn cache_graph_task(
		&self,
		graph_id: &tg::graph::Id,
		progress: crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Load the graph in a blocking task.
		let graph_data = tokio::task::spawn_blocking({
			let server = self.clone();
			let graph_id = graph_id.clone();
			move || {
				let (_size, data) = server
					.store
					.try_get_object_data_sync(&graph_id.into())?
					.ok_or_else(|| tg::error!("failed to load the graph"))?;
				let data: tg::graph::Data = data
					.try_into()
					.map_err(|_| tg::error!("expected graph data"))?;
				Ok::<_, tg::Error>(data)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;

		// Get the items that need cache entries.
		let items = Self::cache_entry_items_for_graph(graph_id, &graph_data)?;
		if items.is_empty() {
			return Ok(());
		}

		// Check if all items already exist in the cache.
		let all_exist = tokio::task::spawn_blocking({
			let server = self.clone();
			let items = items.clone();
			move || {
				for item in &items {
					let path = server.cache_path().join(item.id.to_string());
					if !path.try_exists().unwrap_or(false) {
						return false;
					}
				}
				true
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?;
		if all_exist {
			return Ok(());
		}

		// Write each item to a temp and collect dependencies.
		let outputs = tokio::task::spawn_blocking({
			let server = self.clone();
			let items = items.clone();
			let graph_id = graph_id.clone();
			let progress = progress.clone();
			move || {
				let mut outputs = Vec::new();
				for item in items {
					// Create a temp.
					let temp = Temp::new(&server);

					// Write the item.
					let dependencies = server.cache_write(temp.path(), &item, &progress)?;

					// Filter out same-graph dependencies.
					let dependencies: Vec<Item> = dependencies
						.into_iter()
						.filter(|dependency| dependency.graph.as_ref() != Some(&graph_id))
						.collect();

					// Add the output.
					let output = (item, temp, dependencies);
					outputs.push(output);
				}
				Ok::<_, tg::Error>(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;

		let dependencies: Vec<Item> = outputs
			.iter()
			.flat_map(|(_, _, dependencies)| dependencies.clone())
			.collect();
		future::try_join_all(
			dependencies
				.into_iter()
				.map(|dependency| self.cache_artifact(dependency, progress.clone())),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to cache the dependencies"))?;

		// Rename all entries to the cache directory.
		tokio::task::spawn_blocking({
			let server = self.clone();
			move || {
				for (item, temp, dependencies) in outputs {
					let dependency_ids: Vec<tg::artifact::Id> = dependencies
						.iter()
						.map(|dependency| dependency.id.clone())
						.collect();
					server.cache_rename(item, &temp, &dependency_ids)?;
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;

		Ok(())
	}

	fn cache_write(
		&self,
		path: &Path,
		item: &Item,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<Vec<Item>> {
		// Create the state.
		let mut state = State {
			artifact: item.id.clone(),
			path: path.to_owned(),
			progress: progress.clone(),
			visiting: HashSet::default(),
		};

		// Cache the artifact and collect dependencies.
		let dependencies = self
			.cache_write_artifact(&mut state, path, item)
			.map_err(|source| tg::error!(!source, "failed to write the artifact"))?;

		// Set permissions on the temp directory before rename.
		if state.artifact.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(path, permissions).map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to set permissions"),
			)?;
		}

		Ok(dependencies)
	}

	fn cache_write_artifact(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
	) -> tg::Result<Vec<Item>> {
		// Write the artifact and collect dependencies.
		let dependencies = match &item.node {
			tg::graph::data::Node::Directory(node) => {
				self.cache_directory(state, path, item, node)?
			},
			tg::graph::data::Node::File(node) => self.cache_file(state, path, item, node)?,
			tg::graph::data::Node::Symlink(node) => self.cache_symlink(state, path, item, node)?,
		};

		// Set the file times to the epoch.
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(path, epoch, epoch).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to set the modified time"),
		)?;

		Ok(dependencies)
	}

	fn cache_directory(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::Directory,
	) -> tg::Result<Vec<Item>> {
		let Item { id, graph, .. } = item;

		// Add to the visiting set to detect cycles.
		state.visiting.insert(id.clone());

		// Create the directory.
		std::fs::create_dir_all(path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the directory"),
		)?;

		// Collect all entries, recursively flattening branches.
		let entries =
			crate::directory::collect_directory_entries(&self.store, node, graph.as_ref())?;

		// Recurse into the entries.
		let mut dependencies = Vec::new();
		let mut visited = HashSet::<tg::artifact::Id, tg::id::BuildHasher>::default();
		for (name, mut edge) in entries {
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}
			let path = path.join(&name);
			let item = self
				.cache_get_item(edge)
				.map_err(|source| tg::error!(!source, "failed to get the item"))?;

			// Check for a cycle.
			if state.visiting.contains(&item.id) {
				return Err(tg::error!("detected a directory cycle"));
			}

			let item_id = item.id.clone();
			let entry_dependencies = self
				.cache_write_artifact(state, &path, &item)
				.map_err(|source| tg::error!(!source, "failed to write the artifact"))?;
			if visited.insert(item_id) {
				for dependency in entry_dependencies {
					dependencies.push(dependency);
				}
			}
		}

		// Remove from the visiting set.
		state.visiting.remove(id);

		// Set the permissions.
		let permissions = std::fs::Permissions::from_mode(0o555);
		std::fs::set_permissions(path, permissions).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to set permissions"),
		)?;

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(dependencies)
	}

	fn cache_file(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::File,
	) -> tg::Result<Vec<Item>> {
		let Item { id, graph, .. } = item;

		let mut dependencies = Vec::new();
		let mut visited = HashSet::<tg::artifact::Id, tg::id::BuildHasher>::default();
		for dependency in node.dependencies.values() {
			let Some(dependency) = dependency else {
				continue;
			};

			// Get the edge.
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

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}

			// Get the node.
			let item = self
				.cache_get_item(edge)
				.map_err(|source| tg::error!(!source, "failed to get the item"))?;

			// Collect the dependency if it is not the root artifact.
			if item.id != state.artifact && visited.insert(item.id.clone()) {
				dependencies.push(item);
			}
		}

		let mut done = false;
		let contents = node
			.contents
			.as_ref()
			.ok_or_else(|| tg::error!("missing contents"))?;

		let src = &self.cache_path().join(id.to_string());
		let dst = path;

		// Attempt to hard link the file.
		let hard_link_prohibited = if cfg!(target_os = "macos") {
			dst.to_str()
				.ok_or_else(|| tg::error!("invalid path"))?
				.contains(".app/Contents")
		} else {
			false
		};
		if !hard_link_prohibited {
			let result = std::fs::hard_link(src, dst);
			if result.is_ok()
				|| result.is_err_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists)
			{
				let len = std::fs::symlink_metadata(dst)
					.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
					.len();
				state.progress.increment("bytes", len);

				done = true;
			}
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
			let dependencies_ = node.dependencies.keys().cloned().collect::<Vec<_>>();
			if !dependencies_.is_empty() {
				let dependencies = serde_json::to_vec(&dependencies_).map_err(|source| {
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
			let mode = if node.executable { 0o555 } else { 0o444 };
			let permissions = std::fs::Permissions::from_mode(mode);
			std::fs::set_permissions(path, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(dependencies)
	}

	fn cache_symlink(
		&self,
		state: &mut State,
		path: &Path,
		item: &Item,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<Vec<Item>> {
		let Item { graph, .. } = item;

		// Collect the dependency.
		let mut dependencies = Vec::new();

		// Render the target.
		let target = if let Some(mut edge) = node.artifact.clone() {
			let mut target = PathBuf::new();

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
				&& pointer.graph.is_none()
			{
				pointer.graph = graph.clone();
			}

			// Get the dependency node.
			let item = self
				.cache_get_item(edge)
				.map_err(|source| tg::error!(!source, "failed to get the item"))?;

			if item.id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				let dependency_id = item.id.clone();

				// Collect the dependency.
				dependencies.push(item);

				// Update the target.
				target.push(state.path.parent().unwrap().join(dependency_id.to_string()));
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
		} else if let Some(path) = &node.path {
			path.clone()
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		std::os::unix::fs::symlink(target, path)
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		// Increment the progress.
		state.progress.increment("artifacts", 1);

		Ok(dependencies)
	}

	fn cache_rename(
		&self,
		item: Item,
		temp: &Temp,
		dependencies: &[tg::artifact::Id],
	) -> tg::Result<()> {
		// Create the path.
		let path = self.cache_path().join(item.id.to_string());

		// Rename the temp to the path.
		let result = tangram_util::fs::rename_noreplace_sync(temp, &path);
		let done = match result {
			Ok(()) => false,
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists
						| std::io::ErrorKind::IsADirectory
						| std::io::ErrorKind::PermissionDenied
				) =>
			{
				true
			},
			Err(source) => {
				let src = temp.path().display();
				let dst = path.display();
				let error = tg::error!(!source, %src, %dst, "failed to rename to the cache path");
				return Err(error);
			},
		};

		// Set the permissions.
		if !done && item.id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o555);
			std::fs::set_permissions(&path, permissions).map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to set permissions"),
			)?;
		}

		// Set the modified time.
		if !done {
			let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
			filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to set the modified time"),
			)?;
		}

		// Index the cache entry.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let put_cache_entry_arg = tangram_index::PutCacheEntryArg {
			id: item.id,
			touched_at,
			dependencies: dependencies.to_vec(),
		};
		self.index_tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					if let Err(error) = server
						.index
						.put(tangram_index::PutArg {
							cache_entries: vec![put_cache_entry_arg],
							..Default::default()
						})
						.await
					{
						tracing::error!(error = %error.trace(), "failed to put cache entry to index");
					}
				}
			})
			.detach();

		Ok(())
	}

	fn cache_get_item(&self, edge: tg::graph::data::Edge<tg::artifact::Id>) -> tg::Result<Item> {
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
				let bytes = data.serialize()?;
				let id = tg::artifact::Id::new(node.kind(), &bytes);

				let item = Item {
					id,
					node,
					graph: Some(graph_id),
				};

				Ok(item)
			},

			tg::graph::data::Edge::Object(object_id) => {
				// Load the object.
				let (_size, data) = self
					.store
					.try_get_object_data_sync(&object_id.clone().into())
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

						let item = Item {
							id: object_id,
							node,
							graph: Some(graph_id),
						};

						Ok(item)
					},

					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						let item = Item {
							id: object_id,
							node: tg::graph::data::Node::Directory(node),
							graph: None,
						};
						Ok(item)
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						let item = Item {
							id: object_id,
							node: tg::graph::data::Node::File(node),
							graph: None,
						};
						Ok(item)
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						let item = Item {
							id: object_id,
							node: tg::graph::data::Node::Symlink(node),
							graph: None,
						};
						Ok(item)
					},
				}
			},
		}
	}

	fn cache_entry_items_for_graph(
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

	pub(crate) async fn handle_cache_request(
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
			.cache_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the cache task"))?;

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
