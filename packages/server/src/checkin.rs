use self::state::{Graph, State};
use crate::Server;
use futures::{FutureExt as _, Stream, StreamExt as _};
use indoc::indoc;
use std::{
	os::unix::fs::PermissionsExt as _,
	panic::AssertUnwindSafe,
	path::{Path, PathBuf},
	sync::Arc,
	time::Instant,
};
use tangram_client as tg;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tangram_ignore as ignore;
use tokio_util::task::AbortOnDropHandle;

mod input;
mod lock;
mod object;
mod output;
mod solve;
mod state;

impl Server {
	pub async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		let progress = crate::progress::Handle::new();
		let task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = AssertUnwindSafe(server.checkin_inner(arg, &progress))
					.catch_unwind()
					.await;
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
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	// Check in the artifact.
	async fn checkin_inner(
		&self,
		mut arg: tg::checkin::Arg,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::checkin::Output> {
		// Validate the arg.
		if arg.destructive && arg.ignore {
			return Err(tg::error!("ignore is forbidden for destructive checkins"));
		}

		// Canonicalize the path's parent.
		arg.path = crate::util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = &arg.path.display(), "failed to canonicalize the path's parent"))?;

		// If this is a checkin of a path in the cache directory, then retrieve the corresponding artifact.
		if let Ok(path) = arg.path.strip_prefix(self.cache_path()) {
			let id = path
				.components()
				.next()
				.map(|component| {
					let std::path::Component::Normal(name) = component else {
						return Err(tg::error!("invalid path"));
					};
					name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))
				})
				.ok_or_else(|| tg::error!("cannot check in the cache directory"))??
				.parse()?;
			if path.components().count() == 1 {
				let output = tg::checkin::Output {
					referent: tg::Referent::with_item(id),
				};
				return Ok(output);
			}
			let path = path.components().skip(1).collect::<PathBuf>();
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, path).await?;
			let id = artifact.id();
			let output = tg::checkin::Output {
				referent: tg::Referent::with_item(id),
			};
			return Ok(output);
		}

		// Create the ignorer if necessary.
		let ignorer = if arg.ignore {
			Some(Self::checkin_create_ignorer()?)
		} else {
			None
		};

		// Search for the root.
		let root_path = tg::package::try_get_nearest_package_path_for_path(&arg.path)?
			.unwrap_or(&arg.path)
			.to_owned();
		let mut artifacts_path = None;
		for path in root_path.ancestors() {
			let path = path.join(".tangram/artifacts");
			if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
				artifacts_path.replace(path);
				break;
			}
		}
		let artifacts_path = artifacts_path.unwrap_or_else(|| self.artifacts_path());

		// Read a lock for the path.
		let lock = self
			.try_read_lock_for_path(&root_path)
			.map_err(|source| tg::error!(!source, "failed to read the lock"))?;

		// Create the state.
		let graph = Graph {
			nodes: im::Vector::new(),
			paths: im::HashMap::default(),
			roots: im::OrdMap::new(),
		};
		let mut state = State {
			arg: arg.clone(),
			artifacts_path,
			destructive_fixup_sender: None,
			graph,
			graph_objects: Vec::new(),
			lock,
			ignorer,
			progress: progress.clone(),
		};

		// Spawn the destructive fixup task.
		let destructive_fixup_task = if arg.destructive {
			let (sender, receiver) = std::sync::mpsc::channel::<(PathBuf, std::fs::Metadata)>();
			state.destructive_fixup_sender = Some(sender);
			let task = tokio::task::spawn_blocking(move || {
				Self::checkin_destructive_fixup_task(&receiver)
			})
			.map(|result| result.unwrap());
			Some(task)
		} else {
			None
		};

		// Collect input.
		let start = Instant::now();
		let mut state = tokio::task::spawn_blocking({
			let server = self.clone();
			let root = root_path.clone();
			move || {
				server.checkin_input(&mut state, root)?;
				Ok::<_, tg::Error>(state)
			}
		})
		.await
		.unwrap()?;
		tracing::trace!(elapsed = ?start.elapsed(), "collect input");

		// Remove the destructive fixup sender.
		state.destructive_fixup_sender.take();

		// Remove the ignorer.
		state.ignorer.take();

		// Solve.
		if !(state.arg.deterministic || state.arg.locked) {
			let start = Instant::now();
			self.checkin_solve(&mut state).await?;
			tracing::trace!(elapsed = ?start.elapsed(), "solve");
		}

		// Create blobs.
		let start = Instant::now();
		self.checkin_create_blobs(&mut state).await?;
		tracing::trace!(elapsed = ?start.elapsed(), "create blobs");

		// Create objects.
		let start = Instant::now();
		Self::checkin_create_objects(&mut state)?;
		tracing::trace!(elapsed = ?start.elapsed(), "create objects");

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		let state = Arc::new(state);

		// Await the destructive fixup task.
		if let Some(task) = destructive_fixup_task {
			task.await?;
		}

		// Cache.
		let start = Instant::now();
		self.checkin_cache_task(state.clone(), touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to cache"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "cache");

		// Store.
		let start = Instant::now();
		self.checkin_store_task(&state, touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the objects to the store"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "write objects to store");

		// Publish.
		let start = Instant::now();
		self.checkin_messenger_task(&state, touched_at)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to write the objects to the messenger")
			})?;
		tracing::trace!(elapsed = ?start.elapsed(), "write objects to messenger");

		let state = Arc::into_inner(state).unwrap();

		// Create the lock.
		let start = Instant::now();
		self.checkin_create_lock_task(&state)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "create lock");

		// Find the item.
		let node = state
			.graph
			.paths
			.get(&arg.path)
			.copied()
			.ok_or_else(|| tg::error!("failed to get item"))?;

		// Create the referent.
		let item = state.graph.nodes[node]
			.object
			.as_ref()
			.unwrap()
			.id
			.clone()
			.try_into()
			.unwrap();
		let path = state.graph.nodes[node].path.as_deref().cloned();
		let tag = None;
		let options = tg::referent::Options { path, tag };
		let referent = tg::Referent { item, options };

		// Create the output.
		let output = tg::checkin::Output { referent };

		Ok(output)
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<ignore::Ignorer> {
		let file_names = vec![
			".tangramignore".into(),
			".tgignore".into(),
			".gitignore".into(),
		];
		let global = indoc!(
			"
				.DS_Store
				.git
				tangram.lock
			"
		);
		ignore::Ignorer::new(file_names, Some(global))
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}

	fn checkin_destructive_fixup_task(
		receiver: &std::sync::mpsc::Receiver<(std::path::PathBuf, std::fs::Metadata)>,
	) -> tg::Result<()> {
		while let Ok((path, metadata)) = receiver.recv() {
			Self::set_permissions_and_times(&path, &metadata).map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to set permissions"),
			)?;
		}
		Ok::<_, tg::Error>(())
	}

	fn set_permissions_and_times(path: &Path, metadata: &std::fs::Metadata) -> tg::Result<()> {
		if !metadata.is_symlink() {
			let mode = metadata.permissions().mode();
			let executable = mode & 0o111 != 0;
			let new_mode = if metadata.is_dir() || executable {
				0o555
			} else {
				0o444
			};
			if new_mode != mode {
				let permissions = std::fs::Permissions::from_mode(new_mode);
				std::fs::set_permissions(path, permissions).map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to set the permissions"),
				)?;
			}
		}
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(path, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
		)?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_checkin_request<H>(
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
		let stream = handle.checkin(arg).await?;

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
