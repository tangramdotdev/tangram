use {
	self::state::{FixupMessage, Graph},
	crate::{Server, watch::Watch},
	futures::{FutureExt as _, Stream, StreamExt as _},
	indoc::indoc,
	std::{
		os::unix::fs::PermissionsExt as _,
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
		sync::Arc,
		time::Instant,
	},
	tangram_client as tg,
	tangram_futures::stream::Ext as _,
	tangram_http::{Body, request::Ext as _},
	tangram_ignore as ignore,
	tokio_util::task::AbortOnDropHandle,
};

mod blob;
mod cache;
mod index;
mod input;
mod lock;
mod object;
mod solve;
mod state;
mod store;

pub(crate) use self::state::State;

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
		if arg.options.destructive && arg.options.ignore {
			return Err(tg::error!("ignore is forbidden for destructive checkins"));
		}

		// Canonicalize the path's parent.
		arg.path = tangram_util::fs::canonicalize_parent(&arg.path)
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
			let referent = tg::Referent::with_item(id);
			let output = tg::checkin::Output { referent };
			return Ok(output);
		}

		// Find the root.
		let root = tg::package::try_get_nearest_package_path_for_path(&arg.path)?
			.unwrap_or(&arg.path)
			.to_owned();

		// Capture the watch version at the start of the checkin.
		let version = if arg.options.watch {
			self.watches
				.get(&root)
				.map(|watch| watch.version.load(std::sync::atomic::Ordering::SeqCst))
		} else {
			None
		};

		// Clone or create the state.
		let mut state = if arg.options.watch
			&& let Some(watch) = self.watches.get(&root)
		{
			let state = watch.value().state.lock().unwrap();

			// Create the ignorer if necessary.
			let ignorer = if arg.options.ignore {
				Some(Self::checkin_create_ignorer()?)
			} else {
				None
			};

			// Clone the state.
			State {
				arg: state.arg.clone(),
				artifacts_path: state.artifacts_path.clone(),
				blobs: state.blobs.clone(),
				fixup_sender: None,
				graph: state.graph.clone(),
				ignorer,
				lock: state.lock.clone(),
				objects: state.objects.clone(),
				progress: progress.clone(),
				root_path: state.root_path.clone(),
			}
		} else {
			// Create the ignorer if necessary.
			let ignorer = if arg.options.ignore {
				Some(Self::checkin_create_ignorer()?)
			} else {
				None
			};

			// Try to find the artifacts path.
			let artifacts_path = root.join(".tangram/artifacts");
			let artifacts_path = if tokio::fs::try_exists(&artifacts_path)
				.await
				.is_ok_and(|exists| exists)
			{
				Some(artifacts_path)
			} else {
				None
			};

			// Try to get a lock.
			let lock = Self::checkin_try_read_lock(&root)
				.map_err(|source| tg::error!(!source, "failed to read the lock"))?;

			// Create the state.
			State {
				arg: arg.clone(),
				artifacts_path,
				blobs: im::HashMap::default(),
				fixup_sender: None,
				graph: Graph::default(),
				ignorer,
				lock,
				objects: state::Objects::default(),
				progress: progress.clone(),
				root_path: root.clone(),
			}
		};

		// Spawn the fixup task.
		let fixup_task = if arg.options.destructive {
			let (sender, receiver) = std::sync::mpsc::channel();
			state.fixup_sender = Some(sender);
			let task = tokio::task::spawn_blocking(move || Self::checkin_fixup_task(&receiver))
				.map(|result| result.unwrap());
			Some(task)
		} else {
			None
		};

		// Collect input.
		let start = Instant::now();
		let mut state = tokio::task::spawn_blocking({
			let server = self.clone();
			let root = root.clone();
			move || {
				server.checkin_input(&mut state, root)?;
				Ok::<_, tg::Error>(state)
			}
		})
		.await
		.unwrap()?;
		tracing::trace!(elapsed = ?start.elapsed(), "collect input");

		// Remove the fixup sender.
		state.fixup_sender.take();

		// Remove the ignorer.
		state.ignorer.take();

		// Solve.
		if state.arg.options.solve {
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

		// Write the lock.
		let start = Instant::now();
		self.checkin_write_lock(&state)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "create lock");

		let state = Arc::new(state);

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Cache.
		if let Some(task) = fixup_task {
			task.await?;
		}
		let start = Instant::now();
		self.checkin_cache(state.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to cache"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "cache");

		// Store.
		let start = Instant::now();
		self.checkin_store(&state, touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the objects to the store"))?;
		tracing::trace!(elapsed = ?start.elapsed(), "write objects to store");

		// Find the item.
		let node = state
			.graph
			.paths
			.get(&arg.path)
			.copied()
			.ok_or_else(|| tg::error!("failed to get the item"))?;

		// Create the referent.
		let item = state
			.graph
			.nodes
			.get(&node)
			.unwrap()
			.object_id
			.as_ref()
			.unwrap()
			.clone()
			.try_into()
			.unwrap();
		let options = tg::referent::Options::with_path(arg.path.clone());
		let referent = tg::Referent { item, options };

		// Index and return the state.
		self.tasks.spawn({
			let server = self.clone();
			move |_| {
				async move {
					// Index.
					server.checkin_index(&state, touched_at).await?;

					// Retrieve the state.
					let mut state = Arc::try_unwrap(state).unwrap();

					// Mark all nodes as clean and not visited.
					for (_, node) in state.graph.nodes.iter_mut() {
						node.dirty = false;
						node.visited = false;
					}

					// If the watch option is enabled, then update the state of an existing watch or add a new watch.
					if state.arg.options.watch {
						let entry = server.watches.entry(root.clone());
						match entry {
							dashmap::Entry::Occupied(mut entry) => {
								// Check if the path was modified during the checkin.
								if let Some(version) = version {
									let current = entry
										.get()
										.version
										.load(std::sync::atomic::Ordering::SeqCst);
									if current != version {
										return Err(tg::error!(
											"files were modified during checkin"
										));
									}
								}

								// Update the state.
								*entry.get_mut().state.lock().unwrap() = state;
							},
							dashmap::Entry::Vacant(entry) => {
								let watch = Watch::new(&root, state).map_err(|source| {
									tg::error!(!source, "failed to create the watch")
								})?;
								entry.insert(watch);
							},
						}
					}

					Ok::<_, tg::Error>(())
				}
				.map(|result| {
					if let Err(error) = result {
						tracing::error!(?error, "the index task failed");
					}
				})
			}
		});

		// Create the output.
		let output = tg::checkin::Output { referent };

		Ok(output)
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<ignore::Ignorer> {
		let file_names = vec![".tangramignore".into(), ".gitignore".into()];
		let global = indoc!(
			"
				.DS_Store
				.git
				.tangram
				tangram.lock
			"
		);
		ignore::Ignorer::new(file_names, Some(global))
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}

	fn checkin_fixup_task(receiver: &std::sync::mpsc::Receiver<FixupMessage>) -> tg::Result<()> {
		while let Ok(message) = receiver.recv() {
			Self::set_permissions_and_times(&message.path, &message.metadata).map_err(
				|source| tg::error!(!source, %path = message.path.display(), "failed to set permissions"),
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
