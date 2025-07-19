use crate::Server;
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream};
use num::ToPrimitive as _;
use reflink_copy::reflink;
use std::{
	collections::{HashMap, HashSet},
	os::unix::fs::PermissionsExt as _,
	panic::AssertUnwindSafe,
	path::{Path, PathBuf},
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
		let task = AbortOnDropHandle::new(tokio::spawn({
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
		}));
		let stream = progress.stream().attach(task).right_stream();
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
		let path = crate::util::fs::canonicalize_parent(path)
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
				let edge = tg::graph::data::Edge::Object(id.clone());
				let path = state.path.clone();
				server.checkout_inner(&mut state, &path, &edge)?;

				// Write the lock if necessary.
				server.checkout_write_lock(id, &mut state)?;

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
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
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
		self.checkout_inner(state, &path, edge)?;
		Ok(())
	}

	fn checkout_inner(
		&self,
		state: &mut State,
		path: &Path,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<()> {
		// Get the artifact ID and graph node data.
		let (id, node, graph) = self.checkout_get_node(state, edge)?;

		// Checkout the artifact.
		match node {
			tg::graph::data::Node::Directory(node) => {
				self.checkout_inner_directory(state, path, &id, graph.as_ref(), &node)?;
			},
			tg::graph::data::Node::File(node) => {
				self.checkout_inner_file(state, path, &id, graph.as_ref(), &node)?;
			},
			tg::graph::data::Node::Symlink(node) => {
				self.checkout_inner_symlink(state, path, &id, graph.as_ref(), &node)?;
			},
		}

		Ok(())
	}

	// Look up the underlying graph node of the artifact.
	fn checkout_get_node(
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
				self.checkout_ensure_graph_exists(state, graph)?;

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
				// Otherwise, lookup the artifact data by ID.
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
						let (_, node, graph) = self.checkout_get_node(
							state,
							&tg::graph::data::Edge::Reference(reference),
						)?;
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

	fn checkout_ensure_graph_exists(
		&self,
		state: &mut State,
		graph: &tg::graph::Id,
	) -> tg::Result<()> {
		if state.graphs.contains_key(graph) {
			return Ok(());
		}
		#[allow(clippy::match_wildcard_for_single_variants)]
		let data: tg::graph::Data = match &self.store {
			crate::Store::Lmdb(store) => store.try_get_object_data_sync(&graph.clone().into())?,
			crate::Store::Memory(store) => store.try_get_object_data(&graph.clone().into())?,
			_ => {
				return Err(tg::error!("unimplemented"));
			},
		}
		.ok_or_else(|| tg::error!("expected the object to be stored"))?
		.try_into()
		.map_err(|_| tg::error!("expected a graph"))?;

		state.graphs.insert(graph.clone(), data);

		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn checkout_inner_directory(
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
			if let tg::graph::data::Edge::Reference(reference) = &mut edge {
				if reference.graph.is_none() {
					reference.graph = graph.cloned();
				}
			}
			let path = path.join(name);
			self.checkout_inner(state, &path, &edge)?;
		}

		Ok(())
	}

	fn checkout_inner_file(
		&self,
		state: &mut State,
		path: &Path,
		id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::File,
	) -> tg::Result<()> {
		// Check out the dependencies.
		for referent in node.dependencies.values() {
			// Skip object edges.
			let mut edge = match referent.item.clone() {
				tg::graph::data::Edge::Reference(graph) => tg::graph::data::Edge::Reference(graph),
				tg::graph::data::Edge::Object(id) => match id.try_into() {
					Ok(id) => tg::graph::data::Edge::Object(id),
					Err(_) => continue,
				},
			};

			// Update the graph if necessarsy.
			if let tg::graph::data::Edge::Reference(reference) = &mut edge {
				if reference.graph.is_none() {
					reference.graph = graph.cloned();
				}
			}

			// Get the underlying node ID.
			let (id, _, _) = self.checkout_get_node(state, &edge)?;

			if id != state.artifact {
				self.checkout_dependency(state, &id, &edge)?;
			}
		}

		let src = &self.cache_path().join(id.to_string());
		let dst = &path;

		// Attempt to reflink the file.
		let result = reflink(src, dst);
		if result.is_ok() {
			return Ok(());
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
					.read(&server, tg::blob::read::Arg::default())
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
			xattr::set(dst, tg::file::XATTR_DEPENDENCIES_NAME, &dependencies)
				.map_err(|source| tg::error!(!source, "failed to write the dependencies attr"))?;
		}

		// Set the permissions.
		if node.executable {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(dst, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		Ok(())
	}

	fn checkout_inner_symlink(
		&self,
		state: &mut State,
		path: &Path,
		_id: &tg::artifact::Id,
		graph: Option<&tg::graph::Id>,
		node: &tg::graph::data::Symlink,
	) -> tg::Result<()> {
		let tg::graph::data::Symlink {
			artifact,
			path: path_,
		} = node;

		// Render the target.
		let target = if let Some(mut edge) = artifact.clone() {
			let mut target = PathBuf::new();

			// Update the graph if necessary.
			if let tg::graph::data::Edge::Reference(reference) = &mut edge {
				if reference.graph.is_none() {
					reference.graph = graph.cloned();
				}
			}

			// Get the id.
			let (id, _, _) = self.checkout_get_node(state, &edge)?;

			if id == state.artifact {
				// If the symlink's artifact is the root artifact, then use the root path.
				target.push(&state.path);
			} else {
				// If the symlink's artifact is another artifact, then cache it and use the artifact's path.
				self.checkout_dependency(state, &id, &edge)?;

				// Update the target.
				let artifacts_path = state
					.artifacts_path
					.as_ref()
					.ok_or_else(|| tg::error!("expected there to be an artifacts path"))?;
				target.push(artifacts_path.join(id.to_string()));
			}

			// Add the path if it is set.
			if let Some(path_) = path_ {
				target.push(path_);
			}

			// Diff the path.
			let src = path
				.parent()
				.ok_or_else(|| tg::error!("expected the path to have a parent"))?;
			let dst = &target;
			tg::util::path::diff(src, dst)?
		} else if let Some(path_) = path_.clone() {
			path_
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		std::os::unix::fs::symlink(target, path)
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		Ok(())
	}

	fn checkout_write_lock(&self, id: tg::artifact::Id, state: &mut State) -> tg::Result<()> {
		// Create the lock.
		let lock = self
			.create_lock(&id, state.arg.dependencies)
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;

		// Do not write the lock if it is empty.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Write the lock.
		let artifact = tg::Artifact::with_id(id);
		if artifact.is_directory() {
			let contents = serde_json::to_vec_pretty(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			let lockfile_path = state.path.join(tg::package::LOCKFILE_FILE_NAME);
			std::fs::write(&lockfile_path, &contents).map_err(
				|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write the lockfile"),
			)?;
		} else if artifact.is_file() {
			let contents = serde_json::to_vec(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			xattr::set(&state.path, tg::file::XATTR_LOCK_NAME, &contents)
				.map_err(|source| tg::error!(!source, "failed to write the lockattr"))?;
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
