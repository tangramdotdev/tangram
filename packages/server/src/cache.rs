use {
	crate::{Context, Server, temp::Temp},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	itertools::Itertools as _,
	num::ToPrimitive as _,
	std::{
		collections::{HashMap, HashSet},
		os::unix::fs::PermissionsExt as _,
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
		pin::pin,
	},
	tangram_client::prelude::*,
	tangram_either::Either,
	tangram_futures::{
		stream::{Ext as _, TryExt as _},
		task::Task,
	},
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::prelude::*,
	tangram_util::read::InspectReader,
};

struct State {
	artifact: tg::artifact::Id,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>,
	path: PathBuf,
	progress: crate::progress::Handle<()>,
}

struct GetNodeOutput {
	id: tg::artifact::Id,
	node: tg::graph::data::Node,
	graph: Option<tg::graph::Id>,
}

type Dependency = (tg::graph::data::Edge<tg::artifact::Id>, GetNodeOutput);

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
				// Ensure the artifact is complete.
				let result = server
					.cache_ensure_complete(&artifacts, &progress)
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							?artifacts,
							"failed to ensure the artifacts are complete"
						)
					});
				if let Err(error) = result {
					tracing::warn!(?error);
					progress.log(
						tg::progress::Level::Warning,
						"failed to ensure the artifacts are complete".into(),
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

	async fn cache_ensure_complete(
		&self,
		artifacts: &[tg::artifact::Id],
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Check if the artifacts are complete.
		let complete = future::try_join_all(artifacts.iter().map(|artifact| {
			let server = self.clone();
			let artifact = artifact.clone();
			async move {
				server
					.try_get_object_complete(&artifact.into())
					.await
					.map(Option::unwrap_or_default)
			}
		}))
		.await?
		.iter()
		.all(|complete| *complete);
		if complete {
			return Ok(());
		}

		// Index.
		let stream = self.index().await?;
		let stream = std::pin::pin!(stream);
		stream.try_last().await?;

		// Check if the artifacts are complete.
		let complete = future::try_join_all(artifacts.iter().map(|artifact| {
			let server = self.clone();
			let artifact = artifact.clone();
			async move {
				server
					.try_get_object_complete(&artifact.into())
					.await
					.map(Option::unwrap_or_default)
			}
		}))
		.await?
		.iter()
		.all(|complete| *complete);
		if complete {
			return Ok(());
		}

		// Pull.
		let stream = self
			.pull(tg::pull::Arg {
				items: artifacts
					.iter()
					.map(|artifact| Either::Left(artifact.clone().into()))
					.collect(),
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

	async fn cache_task(
		&self,
		id: &tg::artifact::Id,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Get the node in a blocking task.
		let edge = tg::graph::data::Edge::Object(id.clone());
		let GetNodeOutput { id, node, graph } = tokio::task::spawn_blocking({
			let server = self.clone();
			let edge = edge.clone();
			move || server.cache_get_node(None, &edge)
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		// Get or spawn and await the cache task.
		self.cache_dependency(edge, id, node, graph, progress)
			.await?;

		Ok(())
	}

	async fn cache_dependency(
		&self,
		_edge: tg::graph::data::Edge<tg::artifact::Id>,
		id: tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<tg::graph::Id>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		let task = self.cache_tasks.get_or_spawn_with_context(
			id.clone(),
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
				let server = self.clone();
				let id = id.clone();
				move |dependency_progress, _| {
					let server = server.clone();
					async move {
						server
							.cache_dependency_task(&id, node, graph.as_ref(), &dependency_progress)
							.await
					}
				}
			},
		);

		// Forward progress events from the dependency to our progress handle.
		let mut dependency_artifacts = 0u64;
		let mut dependency_bytes = 0u64;
		let mut stream = pin!(task.context().stream().fuse());
		let mut task_future = pin!(task.wait().fuse());
		loop {
			futures::select! {
				event = stream.next() => {
					if let Some(Ok(tg::progress::Event::Update(indicator))) = event {
						if indicator.name == "artifacts" && let Some(current) = indicator.current {
							progress.increment("artifacts", current.saturating_sub(dependency_artifacts));
							dependency_artifacts = current;
						} else if indicator.name == "bytes" && let Some(current) = indicator.current {
							progress.increment("bytes", current.saturating_sub(dependency_bytes));
							dependency_bytes = current;
						}
					}
				}
				result = task_future => {
					result.unwrap()?;
					break;
				}
			}
		}

		Ok(())
	}

	fn cache_dependency_task(
		&self,
		id: &tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<&tg::graph::Id>,
		progress: &crate::progress::Handle<()>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			// Create the path.
			let path = self.cache_path().join(id.to_string());

			// If the path exists, then return.
			let exists = tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to determine if the path exists"))?;
			if exists {
				return Ok(());
			}

			// Create the temp.
			let temp = Temp::new(self);

			// Write the artifact to the temp and collect dependencies.
			let dependencies = tokio::task::spawn_blocking({
				let server = self.clone();
				let path = temp.path().to_owned();
				let id = id.clone();
				let graph = graph.cloned();
				let progress = progress.clone();
				move || server.cache_dependency_write(&path, &id, node, graph.as_ref(), &progress)
			})
			.await
			.map_err(|source| tg::error!(!source, "the cache task panicked"))??;

			// Recursively cache all dependencies concurrently.
			let server = self.clone();
			future::try_join_all(dependencies.into_iter().map(|(edge, output)| {
				let server = server.clone();
				let progress = progress.clone();
				async move {
					server
						.cache_dependency(edge, output.id, output.node, output.graph, &progress)
						.await
				}
			}))
			.await?;

			// Rename the temp to the cache path.
			let cache_path = self.cache_path().join(id.to_string());
			let result = tokio::fs::rename(&temp, &cache_path).await;
			let done = match result {
				Ok(()) => false,
				Err(error)
					if matches!(
						error.kind(),
						std::io::ErrorKind::AlreadyExists
							| std::io::ErrorKind::DirectoryNotEmpty
							| std::io::ErrorKind::PermissionDenied
					) =>
				{
					true
				},
				Err(source) => {
					let src = temp.path().display();
					let dst = cache_path.display();
					let error =
						tg::error!(!source, %src, %dst, "failed to rename to the cache path");
					return Err(error);
				},
			};

			// Set the permissions.
			if !done && id.is_directory() {
				let permissions = std::fs::Permissions::from_mode(0o555);
				tokio::fs::set_permissions(&cache_path, permissions)
					.await
					.map_err(
						|source| tg::error!(!source, path = %cache_path.display(), "failed to set permissions"),
					)?;
			}

			// Set the modified time.
			if !done {
				tokio::task::spawn_blocking({
						let cache_path = cache_path.clone();
						move || {
					let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
							filetime::set_symlink_file_times(&cache_path, epoch, epoch).map_err(
								|source| tg::error!(!source, path = %cache_path.display(), "failed to set the modified time"),
							)
						}
					})
					.await
					.map_err(|error| tg::error!(!error, "failed to join the task"))??;
			}

			// Publish the put cache entry index message.
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
				async move {
					let message = crate::index::Message::PutCacheEntry(
						crate::index::message::PutCacheEntry { id, touched_at },
					);
					let message = message.serialize()?;
					let _published = server
						.messenger
						.stream_publish("index".to_owned(), message)
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
					Ok::<_, tg::Error>(())
				}
			});

			Ok(())
		}
	}

	fn cache_dependency_write(
		&self,
		path: &Path,
		id: &tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<&tg::graph::Id>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<Vec<Dependency>> {
		// Create the state.
		let mut state = State {
			artifact: id.clone(),
			graphs: HashMap::default(),
			path: path.to_owned(),
			progress: progress.clone(),
		};

		// Cache the artifact and collect dependencies.
		let dependencies = self.cache_artifact(&mut state, path, id, node, graph)?;

		// Set permissions on the temp directory before rename.
		if id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(path, permissions).map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to set permissions"),
			)?;
		}

		Ok(dependencies)
	}

	fn cache_artifact(
		&self,
		state: &mut State,
		path: &Path,
		id: &tg::artifact::Id,
		node: tg::graph::data::Node,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<Vec<Dependency>> {
		// Write the artifact and collect dependencies.
		let dependencies = match node {
			tg::graph::data::Node::Directory(node) => {
				self.cache_directory(state, path, id, graph, &node)?
			},
			tg::graph::data::Node::File(node) => self.cache_file(state, path, id, graph, &node)?,
			tg::graph::data::Node::Symlink(node) => {
				self.cache_symlink(state, path, id, graph, &node)?
			},
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
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Directory,
	) -> tg::Result<Vec<Dependency>> {
		// Create the directory.
		std::fs::create_dir_all(path).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the directory"),
		)?;

		// Recurse into the entries.
		let mut dependencies = Vec::new();
		let mut visited = HashSet::<tg::artifact::Id, tg::id::BuildHasher>::default();
		for (name, edge) in &node.entries {
			let mut edge = edge.clone();
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}
			let path = path.join(name);
			let GetNodeOutput { id, node, graph } =
				self.cache_get_node(Some(&mut state.graphs), &edge)?;
			let entry_dependencies =
				self.cache_artifact(state, &path, &id, node, graph.as_ref())?;
			if visited.insert(id) {
				for dependency in entry_dependencies {
					dependencies.push(dependency);
				}
			}
		}

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
		id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::File,
	) -> tg::Result<Vec<Dependency>> {
		let mut dependencies = Vec::new();
		let mut visited = HashSet::<tg::artifact::Id, tg::id::BuildHasher>::default();
		for referent in node.dependencies.values() {
			let Some(referent) = referent else {
				continue;
			};

			// Get the edge.
			let mut edge = match referent.item.clone() {
				tg::graph::data::Edge::Reference(graph) => tg::graph::data::Edge::Reference(graph),
				tg::graph::data::Edge::Object(id) => match id.try_into() {
					Ok(id) => tg::graph::data::Edge::Object(id),
					Err(_) => continue,
				},
			};

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}

			// Get the node.
			let output = self.cache_get_node(Some(&mut state.graphs), &edge)?;

			// Collect the dependency if it is not the root artifact.
			if output.id != state.artifact && visited.insert(output.id.clone()) {
				dependencies.push((edge.clone(), output));
			}
		}

		let mut done = false;
		let contents = node
			.contents
			.as_ref()
			.ok_or_else(|| tg::error!("missing contents"))?;

		let src = &self.cache_path().join(id.to_string());
		let dst = &path;

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
			let mut file = std::fs::File::create(dst)
				.map_err(|source| tg::error!(!source, path = ?dst, "failed to create the file"))?;
			std::io::copy(&mut reader, &mut file).map_err(
				|source| tg::error!(!source, path = ?dst, "failed to write to the file"),
			)?;

			// Set the dependencies attr.
			let dependencies_ = node.dependencies.keys().cloned().collect::<Vec<_>>();
			if !dependencies_.is_empty() {
				let dependencies = serde_json::to_vec(&dependencies_).map_err(|source| {
					tg::error!(!source, "failed to serialize the dependencies")
				})?;
				xattr::set(dst, tg::file::DEPENDENCIES_XATTR_NAME, &dependencies).map_err(
					|source| tg::error!(!source, "failed to write the dependencies attr"),
				)?;
			}

			// Set the permissions.
			let mode = if node.executable { 0o555 } else { 0o444 };
			let permissions = std::fs::Permissions::from_mode(mode);
			std::fs::set_permissions(dst, permissions)
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
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<Vec<Dependency>> {
		// Collect the dependency.
		let mut dependencies = Vec::new();

		// Render the target.
		let target = if let Some(mut edge) = node.artifact.clone() {
			let mut target = PathBuf::new();

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}

			// Get the dependency node.
			let output = self.cache_get_node(Some(&mut state.graphs), &edge)?;

			if output.id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				let dependency_id = output.id.clone();

				// Collect the dependency.
				dependencies.push((edge.clone(), output));

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

	fn cache_get_node(
		&self,
		graphs: Option<&mut HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>>,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
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
					let (_size, data) = self
						.store
						.try_get_object_data_sync(&graph.clone().into())?
						.ok_or_else(|| tg::error!("failed to load the graph"))?;
					let data = data
						.try_into()
						.map_err(|_| tg::error!("expected graph data"))?;
					graphs.insert(graph.clone(), data);
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
				let bytes = data.serialize()?;
				let id = tg::artifact::Id::new(node.kind(), &bytes);

				Ok(GetNodeOutput {
					id,
					node,
					graph: Some(graph.clone()),
				})
			},

			tg::graph::data::Edge::Object(id) => {
				// Load the object.
				let (_size, data) = self
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
							let (_size, data) = self
								.store
								.try_get_object_data_sync(&graph.clone().into())?
								.ok_or_else(|| tg::error!("failed to load the graph"))?;
							let data = data
								.try_into()
								.map_err(|_| tg::error!("expected graph data"))?;
							graphs.insert(graph.clone(), data);
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
						})
					},

					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						Ok(GetNodeOutput {
							id: id.clone(),
							node: tg::graph::data::Node::Directory(node),
							graph: None,
						})
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						Ok(GetNodeOutput {
							id: id.clone(),
							node: tg::graph::data::Node::File(node),
							graph: None,
						})
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						Ok(GetNodeOutput {
							id: id.clone(),
							node: tg::graph::data::Node::Symlink(node),
							graph: None,
						})
					},
				}
			},
		}
	}

	pub(crate) async fn handle_cache_request(
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
		let stream = self.cache_with_context(context, arg).await?;

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
