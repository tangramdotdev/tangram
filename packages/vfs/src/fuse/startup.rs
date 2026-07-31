use super::{
	connection::Connection,
	read_write::ReadWriteStartupContext,
	ring::{RingConfig, RingStartupContext},
	*,
};

const IO_URING_ENTRIES: u32 = 256;
const SQPOLL_IDLE_MS: u32 = 2_000;

type SqpollRing = IoUring<io_uring::squeue::Entry128>;

struct Mount {
	connection_id: Option<u64>,
	fd: Arc<OwnedFd>,
}

struct StartupConfig {
	limits: RequestLimits,
	options: Options,
	ring_config: Option<RingConfig>,
}

struct Transport {
	dispatcher: Option<tokio::task::JoinHandle<()>>,
	threads: Vec<std::thread::JoinHandle<()>>,
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub async fn start(
		provider: P,
		path: &Path,
		options: Options,
		recvfd: OwnedFd,
	) -> Result<Self> {
		// Receive the mounted FUSE descriptor.
		let mount = Self::receive_mount(recvfd).await?;
		let connection_id = mount.connection_id;

		let result = Self::start_inner(provider, path, options, mount).await;

		// Unmount if we encounter an error. A sandbox's mount lives in its mount namespace and cannot be unmounted from the host.
		if result.is_err() && connection_id.is_none() {
			Self::unmount(path).await.ok();
		}

		result
	}

	async fn receive_mount(recvfd: OwnedFd) -> Result<Mount> {
		let (fd, connection_id) = tokio::task::spawn_blocking(move || Self::mount(&recvfd))
			.await
			.map_err(Error::other)?
			.inspect_err(|error| tracing::error!(%error, "failed to mount"))?;
		let mount = Mount { connection_id, fd };
		Ok(mount)
	}

	async fn start_inner(
		provider: P,
		path: &Path,
		mut options: Options,
		mount: Mount,
	) -> Result<Self> {
		// Select ReadWrite before initializing an external connection because it cannot be remounted for a fallback.
		let auto_unmount = mount.connection_id.is_none();
		if !auto_unmount && options.io == Io::Auto {
			options.io = Io::ReadWrite;
		}

		// Prepare the FUSE connection.
		let supports_no_opendir = provider.supports_no_opendir();
		let mut config = Self::startup_config(options)?;
		let (mut connection, mut sqpoll_ring) =
			Self::prepare_connection(path, supports_no_opendir, &mut config, mount).await?;

		// Create the server state.
		let server = Self(Arc::new(State {
			active_requests: Mutex::new(HashMap::new()),
			auto_unmount,
			no_opendir_support: connection.features.no_opendir_support,
			passthrough_backings: Mutex::new(PassthroughBackings::default()),
			passthrough_enabled: connection.features.passthrough,
			passthrough_permission_warning_emitted: AtomicBool::new(false),
			passthrough_required: config.options.passthrough == Passthrough::Required,
			pending_response_resources: Mutex::new(HashMap::new()),
			provider,
			task: Mutex::new(None),
		}));

		// Start the negotiated transport.
		let runtime = tokio::runtime::Handle::current();
		let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let transport = if connection.features.over_io_uring {
			let ring_config = config
				.ring_config
				.ok_or_else(|| Error::other("missing the io_uring transport configuration"))?;
			let context = RingStartupContext {
				connection_id: connection.id,
				event_receiver: &mut event_receiver,
				event_sender: event_sender.clone(),
				fd: connection.fd.clone(),
				path,
				ring_config,
				runtime: runtime.clone(),
				sqpoll_wq_fd: sqpoll_ring.as_ref().unwrap().as_raw_fd(),
			};
			match server.start_ring_transport(context).await {
				Err(failure) if config.options.io == Io::Auto => {
					// Reconnect with ReadWrite after the io_uring startup failure.
					if !failure.disconnected {
						return Err(Error::other(format!(
							"failed to stop the io_uring transport after startup failed: {}",
							failure.error,
						)));
					}
					tracing::warn!(
						error = %failure.error,
						"failed to start the FUSE io_uring transport; falling back to ReadWrite",
					);
					drop(sqpoll_ring.take());
					config.options.io = Io::ReadWrite;
					config.limits =
						Self::request_limits(rustix::param::page_size(), DEFAULT_MAX_WRITE)?;
					let mount = Self::remount(path).await?;
					connection = Self::connect(
						path,
						config.options,
						config.limits,
						supports_no_opendir,
						mount.fd,
						mount.connection_id,
					)
					.await?;
					server.validate_fallback_features(connection.features)?;
					let context = ReadWriteStartupContext {
						connection: &connection,
						event_receiver: &mut event_receiver,
						event_sender: event_sender.clone(),
						limits: config.limits,
						path,
					};
					let (threads, dispatcher) = server.start_read_write_transport(context).await?;
					Transport {
						dispatcher: Some(dispatcher),
						threads,
					}
				},
				Err(failure) => return Err(failure.error),
				Ok(threads) => Transport {
					dispatcher: None,
					threads,
				},
			}
		} else {
			let context = ReadWriteStartupContext {
				connection: &connection,
				event_receiver: &mut event_receiver,
				event_sender: event_sender.clone(),
				limits: config.limits,
				path,
			};
			let (threads, dispatcher) = server.start_read_write_transport(context).await?;
			Transport {
				dispatcher: Some(dispatcher),
				threads,
			}
		};
		drop(event_sender);

		// Define the transport shutdown sequence.
		let path = path.to_owned();
		let shutdown_server = server.clone();
		let shutdown = async move {
			let Transport {
				dispatcher,
				mut threads,
			} = transport;
			shutdown_server.cancel_async_requests();
			let disconnected = shutdown_server
				.disconnect_transport(&path, connection.id)
				.await;
			Self::join_transport_threads(&mut threads, disconnected);
			if let Some(dispatcher) = dispatcher {
				if !disconnected {
					dispatcher.abort();
				}
				dispatcher.await.ok();
			}
			if disconnected {
				shutdown_server.rollback_all_response_resources(connection.fd.as_ref());
			}
			drop(connection);
			drop(sqpoll_ring);
		};

		// Start the transport supervisor.
		let task = tangram_futures::task::Shared::spawn(|stop| async move {
			tokio::select! {
				() = stop.wait() => {},
				event = event_receiver.recv() => {
					if let Some(WorkerEvent::Failed { error, worker }) = event {
						tracing::error!(%error, %worker, "a FUSE transport worker failed");
					}
				},
			}
			shutdown.await;
		});
		server.task.lock().unwrap().replace(task);

		Ok(server)
	}

	fn startup_config(mut options: Options) -> Result<StartupConfig> {
		// Configure the requested transport.
		let page_size = rustix::param::page_size();
		let ring_config = if options.io == Io::ReadWrite {
			None
		} else {
			match Self::ring_config(page_size) {
				Err(error) if options.io == Io::Auto => {
					tracing::warn!(
						%error,
						"failed to configure the FUSE io_uring transport; falling back to ReadWrite",
					);
					options.io = Io::ReadWrite;
					None
				},
				Err(error) => return Err(error),
				Ok(config) => Some(config),
			}
		};

		// Select the request limits.
		let limits = match ring_config {
			None => Self::request_limits(page_size, DEFAULT_MAX_WRITE)?,
			Some(config) => config.limits,
		};

		Ok(StartupConfig {
			limits,
			options,
			ring_config,
		})
	}

	async fn prepare_connection(
		path: &Path,
		supports_no_opendir: bool,
		config: &mut StartupConfig,
		mut mount: Mount,
	) -> Result<(Connection, Option<SqpollRing>)> {
		// Attempt to connect.
		loop {
			// Create the connection.
			let connection = Self::connect(
				path,
				config.options,
				config.limits,
				supports_no_opendir,
				mount.fd.clone(),
				mount.connection_id,
			)
			.await?;

			// If io_uring is unsupported fall back to normal i/o.
			if !connection.features.over_io_uring {
				return Ok((connection, None));
			}

			match Self::build_sqpoll_ring() {
				Err(error) if config.options.io == Io::Auto => {
					tracing::warn!(
						%error,
						"failed to build the FUSE io_uring SQPOLL ring; falling back to the ReadWrite transport",
					);
					drop(connection);
					config.options.io = Io::ReadWrite;
					config.ring_config = None;
					config.limits =
						Self::request_limits(rustix::param::page_size(), DEFAULT_MAX_WRITE)?;
					mount = Self::remount(path).await?;
				},
				Err(error) => return Err(error),
				Ok(ring) => return Ok((connection, Some(ring))),
			}
		}
	}

	fn build_sqpoll_ring() -> Result<SqpollRing> {
		let mut builder = IoUring::<io_uring::squeue::Entry128>::builder();
		builder.setup_sqpoll(SQPOLL_IDLE_MS).setup_no_sqarray();
		builder
			.build(IO_URING_ENTRIES)
			.map_err(|error| Error::other(format!("failed to build an SQPOLL ring: {error}")))
	}

	async fn remount(path: &Path) -> Result<Mount> {
		Self::unmount(path).await.ok();
		let recvfd = fusermount3(path)?;
		let mount = Self::receive_mount(recvfd).await?;
		Ok(mount)
	}

	fn validate_fallback_features(&self, features: Features) -> Result<()> {
		if features.no_opendir_support != self.no_opendir_support
			|| features.passthrough != self.passthrough_enabled
		{
			return Err(Error::other(
				"the ReadWrite fallback negotiated different FUSE features",
			));
		}

		Ok(())
	}
}
