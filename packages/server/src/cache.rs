use {
	crate::{Server, handle::ServerOrProxy, temp::Temp},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	itertools::Itertools as _,
	std::{
		collections::HashMap,
		os::unix::fs::PermissionsExt as _,
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
	},
	tangram_client::{self as tg, Handle as _},
	tangram_either::Either,
	tangram_futures::stream::{Ext as _, TryExt as _},
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::prelude::*,
	tokio_util::task::AbortOnDropHandle,
};

struct State {
	artifact: tg::artifact::Id,
	depth: usize,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>,
	path: PathBuf,
	progress: crate::progress::Handle<()>,
	visited: im::HashSet<tg::artifact::Id, tg::id::BuildHasher>,
}

impl Server {
	pub async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let tg::cache::Arg { artifacts } = arg;
		if artifacts.is_empty() {
			return Ok(stream::once(future::ok(tg::progress::Event::Output(()))).left_stream());
		}
		let progress = crate::progress::Handle::new();
		let task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
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

				let _metadata = future::try_join_all(artifacts.iter().map(|artifact| async {
					server
						.try_get_object_metadata(
							&artifact.clone().into(),
							tg::object::metadata::Arg::default(),
						)
						.await
				}))
				.await
				.ok()
				.filter(|metadata| !metadata.is_empty())
				.map(|metadata| {
					metadata.into_iter().fold(
						tg::object::Metadata {
							count: Some(0),
							depth: Some(0),
							weight: Some(0),
						},
						|a, b| {
							let count = a
								.count
								.zip(b.as_ref().and_then(|b| b.count))
								.map(|(a, b)| a + b);
							let depth = a
								.depth
								.zip(b.as_ref().and_then(|b| b.depth))
								.map(|(a, b)| a.max(b));
							let weight = a
								.weight
								.zip(b.as_ref().and_then(|b| b.weight))
								.map(|(a, b)| a + b);
							tg::object::Metadata {
								count,
								depth,
								weight,
							}
						},
					)
				});
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
		}));
		let stream = progress.stream().attach(task).right_stream();
		Ok(stream)
	}

	pub(crate) async fn cache_ensure_complete(
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

		// Create a future to pull the artifacts.
		let pull_future = {
			let progress = progress.clone();
			let server = self.clone();
			async move {
				let stream = server
					.pull(tg::pull::Arg {
						items: artifacts
							.iter()
							.map(|artifact| Either::Right(artifact.clone().into()))
							.collect(),
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

		// Create a future to index then check if the artifacts are complete.
		let index_future = {
			let server = self.clone();
			async move {
				let stream = server.index().await?;
				let stream = std::pin::pin!(stream);
				stream.try_last().await?;
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

	pub(crate) async fn cache_task(
		&self,
		artifact: &tg::artifact::Id,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		let server = self.clone();
		let id = artifact.clone();
		let edge = tg::graph::data::Edge::Object(artifact.clone());
		let visited = im::HashSet::default();
		let progress = progress.clone();
		let task = self
			.cache_tasks
			.get_or_spawn_blocking(id.clone(), move |_| {
				server.cache_dependency_inner(&id, &edge, visited, &progress)
			});
		let future = task.wait().map(|result| match result {
			Ok(result) => Ok(result),
			Err(error) if error.is_cancelled() => Ok(Err(tg::error!("the task was canceled"))),
			Err(error) => Err(error),
		});
		future.await.unwrap()?;
		Ok(())
	}

	fn cache_dependency(
		&self,
		id: &tg::artifact::Id,
		edge: tg::graph::data::Edge<tg::artifact::Id>,
		visited: &im::HashSet<tg::artifact::Id, tg::id::BuildHasher>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		let server = self.clone();
		let id = id.clone();
		let mut visited = visited.clone();
		let progress = progress.clone();
		if visited.insert(id.clone()).is_some() {
			return Ok(());
		}
		let task = self
			.cache_tasks
			.get_or_spawn_blocking(id.clone(), move |_| {
				server.cache_dependency_inner(&id, &edge, visited, &progress)
			});
		let future = task.wait().map(|result| match result {
			Ok(result) => Ok(result),
			Err(error) if error.is_cancelled() => Ok(Err(tg::error!("the task was canceled"))),
			Err(error) => Err(error),
		});
		tokio::runtime::Handle::current()
			.block_on(future)
			.unwrap()?;
		Ok(())
	}

	fn cache_dependency_inner(
		&self,
		id: &tg::artifact::Id,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		visited: im::HashSet<tg::artifact::Id, tg::id::BuildHasher>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Create the path.
		let path = self.cache_path().join(id.to_string());

		// If the path exists, then return.
		let exists = std::fs::exists(&path)
			.map_err(|source| tg::error!(!source, "failed to determine if the path exists"))?;
		if exists {
			return Ok(());
		}

		// Create the temp.
		let temp = Temp::new(self);

		// Create the state.
		let mut state = State {
			artifact: id.clone(),
			depth: 0,
			graphs: HashMap::default(),
			path: temp.path().to_owned(),
			progress: progress.clone(),
			visited,
		};

		// Cache the artifact.
		self.cache_inner(&mut state, temp.path(), edge)?;

		// Rename the temp to the path.
		let src = temp.path();
		let dst = &path;
		if id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(src, permissions).map_err(
				|source| tg::error!(!source, %path = src.display(), "failed to set permissions"),
			)?;
		}
		let result = std::fs::rename(src, dst);
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
				let src = src.display();
				let dst = dst.display();
				let error = tg::error!(!source, %src, %dst, "failed to rename to the cache path");
				return Err(error);
			},
		};
		if !done && id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o555);
			std::fs::set_permissions(dst, permissions).map_err(
				|source| tg::error!(!source, %path = dst.display(), "failed to set permissions"),
			)?;
		}
		if !done {
			let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
			filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
				|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
			)?;
		}

		// Publish the put cache entry index message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
			async move {
				let message =
					crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
						id,
						touched_at,
					});
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

	fn cache_inner(
		&self,
		state: &mut State,
		path: &Path,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<()> {
		// Get the artifact ID and graph node data.
		let (id, node, graph) = self.cache_get_node(state, edge)?;

		// Cache the artifact.
		match node {
			tg::graph::data::Node::Directory(node) => {
				self.cache_inner_directory(state, path, &id, graph.as_ref(), &node)?;
			},
			tg::graph::data::Node::File(node) => {
				self.cache_inner_file(state, path, &id, graph.as_ref(), &node)?;
			},
			tg::graph::data::Node::Symlink(node) => {
				self.cache_inner_symlink(state, path, &id, graph.as_ref(), &node)?;
			},
		}

		// Set the file times to the epoch.
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(path, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
		)?;

		Ok(())
	}

	fn cache_get_node(
		&self,
		state: &mut State,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<(
		tg::artifact::Id,
		tg::graph::data::Node,
		Option<tg::graph::Id>,
	)> {
		match edge {
			// If this is a reference, load the graph and find it.
			tg::graph::data::Edge::Reference(reference) => {
				// Get the graph.
				let graph = reference
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?;

				// Ensure the graph is cached.
				if !state.graphs.contains_key(graph) {
					#[allow(clippy::match_wildcard_for_single_variants)]
					let data: tg::graph::Data = match &self.store {
						crate::Store::Lmdb(store) => {
							store.try_get_object_data_sync(&graph.clone().into())?
						},
						crate::Store::Memory(store) => {
							store.try_get_object_data(&graph.clone().into())?
						},
						_ => {
							return Err(tg::error!("unimplemented"));
						},
					}
					.ok_or_else(|| tg::error!("expected the object to be stored"))?
					.try_into()
					.map_err(|_| tg::error!("expected a graph"))?;
					state.graphs.insert(graph.clone(), data);
				}

				// Get the node.
				let node = state
					.graphs
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
				// Otherwise, look up the artifact data by ID.
				#[allow(clippy::match_wildcard_for_single_variants)]
				let data = match &self.store {
					crate::Store::Lmdb(store) => {
						store.try_get_object_data_sync(&id.clone().into())?
					},
					crate::Store::Memory(store) => store.try_get_object_data(&id.clone().into())?,
					_ => {
						return Err(tg::error!("unimplemented"));
					},
				}
				.ok_or_else(
					|| tg::error!(%root = state.artifact, %id = id.clone(), "expected the object to be stored"),
				)?;
				let data = tg::artifact::Data::try_from(data)?;
				match data {
					// Handle the case where this points into a graph.
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Reference(
						reference,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Reference(reference))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Reference(
						reference,
					)) => {
						let (_, node, graph) = self
							.cache_get_node(state, &tg::graph::data::Edge::Reference(reference))?;
						Ok((id.clone(), node, graph))
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

	fn cache_inner_directory(
		&self,
		state: &mut State,
		path: &Path,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Directory,
	) -> tg::Result<()> {
		// Create the directory.
		std::fs::create_dir_all(path).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to create the directory"),
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
			state.depth += 1;
			self.cache_inner(state, &path, &edge)?;
			state.depth -= 1;
		}

		// Set the permissions.
		let permissions = std::fs::Permissions::from_mode(0o555);
		std::fs::set_permissions(path, permissions).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to set permissions"),
		)?;

		Ok(())
	}

	fn cache_inner_file(
		&self,
		state: &mut State,
		path: &Path,
		id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::File,
	) -> tg::Result<()> {
		// Cache the dependencies.
		for referent in node.dependencies.values() {
			let Some(referent) = referent else {
				continue;
			};
			// Skip object edges.
			let mut edge = match referent.item.clone() {
				tg::graph::data::Edge::Reference(graph) => tg::graph::data::Edge::Reference(graph),
				tg::graph::data::Edge::Object(id) => match id.try_into() {
					Ok(id) => tg::graph::data::Edge::Object(id),
					Err(_) => continue,
				},
			};

			// Update the graph if necessarsy.
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}

			// Get the underlying node ID.
			let (id, _, _) = self.cache_get_node(state, &edge)?;

			// Recurse.
			if id != state.artifact {
				self.cache_dependency(&id, edge, &state.visited, &state.progress)?;
			}
		}

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
				return Ok(());
			}
		}

		// Attempt to write the file.
		let result = tokio::runtime::Handle::current().block_on({
			let server = self.clone();
			async move {
				let contents = node
					.contents
					.as_ref()
					.ok_or_else(|| tg::error!("missing contents"))?;
				let mut reader = tg::Blob::with_id(contents.clone())
					.read(&server, tg::read::Options::default())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
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
		let mode = if node.executable { 0o555 } else { 0o444 };
		let permissions = std::fs::Permissions::from_mode(mode);
		std::fs::set_permissions(dst, permissions)
			.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;

		Ok(())
	}

	fn cache_inner_symlink(
		&self,
		state: &mut State,
		path: &Path,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<()> {
		// Render the target.
		let target = if let Some(mut edge) = node.artifact.clone() {
			let mut target = PathBuf::new();

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Reference(reference) = &mut edge
				&& reference.graph.is_none()
			{
				reference.graph = graph.cloned();
			}

			// Get the id.
			let (id, _, _) = self.cache_get_node(state, &edge)?;

			if id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				// Cache the dependency.
				self.cache_dependency(&id, edge, &state.visited, &state.progress)?;

				// Update the target.
				target.push(state.path.parent().unwrap().join(id.to_string()));
			}

			// Add the path if it is set.
			if let Some(path_) = &node.path {
				target.push(path_);
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

		Ok(())
	}

	pub(crate) async fn handle_cache_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.cache(arg).await?;

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
