use super::{connection::Connection, read_write::ReadWriteStartupContext, *};

type SqpollRing = IoUring<io_uring::squeue::Entry128>;

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
	pub async fn start(provider: P, path: &Path, options: Options) -> Result<Self> {
		let supports_no_opendir = provider.supports_no_opendir();
		let mut config = Self::startup_config(options)?;
		let (mut connection, mut sqpoll_ring) =
			Self::prepare_connection(path, supports_no_opendir, &mut config).await?;

		let server = Self(Arc::new(State {
			active_requests: Mutex::new(HashMap::new()),
			no_opendir_support: connection.features.no_opendir_support,
			passthrough_backing_ids: Mutex::new(HashMap::default()),
			passthrough_enabled: connection.features.passthrough,
			passthrough_permission_warning_emitted: AtomicBool::new(false),
			passthrough_required: config.options.passthrough == Passthrough::Required,
			pending_response_resources: Mutex::new(HashMap::new()),
			provider,
			task: Mutex::new(None),
		}));

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
				Ok(threads) => Transport {
					dispatcher: None,
					threads,
				},
				Err(failure) if config.options.io == Io::Auto => {
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
					connection =
						Self::connect(path, config.options, config.limits, supports_no_opendir)
							.await?;
					if let Err(error) = server.validate_fallback_features(connection.features) {
						drop(connection);
						Self::unmount(path).await.ok();
						return Err(error);
					}
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

		let path = path.to_owned();
		let shutdown_server = server.clone();
		let shutdown = async move {
			let Transport {
				dispatcher,
				mut threads,
			} = transport;
			shutdown_server.cancel_async_requests();
			let disconnected = Self::disconnect_transport(&path, connection.id).await;
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
		let page_size = rustix::param::page_size();
		let ring_config = if options.io == Io::ReadWrite {
			None
		} else {
			match Self::ring_config(page_size) {
				Ok(config) => Some(config),
				Err(error) if options.io == Io::Auto => {
					tracing::warn!(
						%error,
						"failed to configure the FUSE io_uring transport; falling back to ReadWrite",
					);
					options.io = Io::ReadWrite;
					None
				},
				Err(error) => return Err(error),
			}
		};
		let limits = match ring_config {
			Some(config) => config.limits,
			None => Self::request_limits(page_size, DEFAULT_MAX_WRITE)?,
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
	) -> Result<(Connection, Option<SqpollRing>)> {
		loop {
			let connection =
				Self::connect(path, config.options, config.limits, supports_no_opendir).await?;
			if !connection.features.over_io_uring {
				return Ok((connection, None));
			}

			match Self::build_sqpoll_ring() {
				Ok(ring) => return Ok((connection, Some(ring))),
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
				},
				Err(error) => {
					drop(connection);
					Self::unmount(path).await.ok();
					return Err(error);
				},
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
