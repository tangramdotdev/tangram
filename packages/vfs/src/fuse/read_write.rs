use {super::*, tokio::task::JoinSet};

const READ_WRITE_ASYNC_CONCURRENCY: usize = 64;
const READ_WRITE_ASYNC_QUEUE_DEPTH: usize = 64;

pub(super) struct ReadWriteRequest {
	pub(super) fd: Arc<OwnedFd>,
	pub(super) request: Request,
	pub(super) token: CancellationToken,
}

pub(super) struct ReadWriteStartupContext<'a> {
	pub(super) connection: &'a connection::Connection,
	pub(super) event_receiver: &'a mut tokio::sync::mpsc::UnboundedReceiver<WorkerEvent>,
	pub(super) event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	pub(super) limits: RequestLimits,
	pub(super) path: &'a Path,
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) async fn start_read_write_transport(
		&self,
		context: ReadWriteStartupContext<'_>,
	) -> Result<(
		Vec<std::thread::JoinHandle<()>>,
		tokio::task::JoinHandle<()>,
	)> {
		let ReadWriteStartupContext {
			connection,
			event_receiver,
			event_sender,
			limits,
			path,
		} = context;

		// Prepare the readers and dispatcher.
		let reader_fds = match Self::clone_read_write_fds(&connection.fd) {
			Err(error) => {
				Self::disconnect_transport(path, connection.id).await;
				return Err(error);
			},
			Ok(reader_fds) => reader_fds,
		};
		let reader_count = reader_fds.len();
		let (request_sender, request_receiver) =
			tokio::sync::mpsc::channel(READ_WRITE_ASYNC_QUEUE_DEPTH);
		let dispatcher = self.spawn_read_write_dispatcher(request_receiver, event_sender.clone());
		tracing::info!(
			async_concurrency = READ_WRITE_ASYNC_CONCURRENCY,
			async_queue_depth = READ_WRITE_ASYNC_QUEUE_DEPTH,
			reader_count,
			"started the FUSE ReadWrite transport",
		);

		// Start the readers.
		let mut startup_error = None;
		let mut thread_handles = Vec::with_capacity(reader_count);
		for (reader_id, reader_fd) in reader_fds.into_iter().enumerate() {
			let request_sender = request_sender.clone();
			let server = self.clone();
			let worker_event_sender = event_sender.clone();
			let thread = std::thread::Builder::new()
				.name(format!("tangram-fuse-read-write-{reader_id}"))
				.spawn(move || {
					worker_event_sender.send(WorkerEvent::Ready).ok();
					if let Err(error) = server.thread_loop_read_write(
						&reader_fd,
						limits.request_buffer_size,
						&request_sender,
					) {
						if error.raw_os_error() == Some(libc::ENOTCONN) {
							tracing::debug!(%error, %reader_id, "ReadWrite reader exited during shutdown");
						} else {
							tracing::error!(%error, %reader_id, "ReadWrite reader failed");
							worker_event_sender
								.send(WorkerEvent::Failed {
									error,
									worker: format!("ReadWrite reader {reader_id}"),
								})
								.ok();
						}
					}
				});
			match thread {
				Err(error) => {
					startup_error = Some(Error::other(format!(
						"failed to spawn the ReadWrite reader {reader_id}: {error}",
					)));
					break;
				},
				Ok(thread) => thread_handles.push(thread),
			}
		}
		drop(request_sender);

		// Wait for the readers to become ready.
		if startup_error.is_none() {
			for _ in 0..thread_handles.len() {
				match event_receiver.recv().await {
					None => {
						startup_error =
							Some(Error::other("a ReadWrite reader failed during startup"));
						break;
					},
					Some(WorkerEvent::Failed { error, worker }) => {
						startup_error = Some(Error::other(format!(
							"{worker} failed during startup: {error}"
						)));
						break;
					},
					Some(WorkerEvent::Ready) => {},
				}
			}
		}

		// Clean up a partial startup.
		if let Some(error) = startup_error {
			self.cancel_async_requests();
			let disconnected = Self::disconnect_transport(path, connection.id).await;
			Self::join_transport_threads(&mut thread_handles, disconnected);
			if !disconnected {
				dispatcher.abort();
			}
			dispatcher.await.ok();
			if disconnected {
				self.rollback_all_response_resources(connection.fd.as_ref());
			}

			return Err(error);
		}

		Ok((thread_handles, dispatcher))
	}

	pub(super) fn spawn_read_write_dispatcher(
		&self,
		receiver: tokio::sync::mpsc::Receiver<ReadWriteRequest>,
		worker_event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	) -> tokio::task::JoinHandle<()> {
		let server = self.clone();
		tokio::spawn(async move {
			server
				.run_read_write_dispatcher(receiver, worker_event_sender)
				.await;
		})
	}

	async fn run_read_write_dispatcher(
		&self,
		mut receiver: tokio::sync::mpsc::Receiver<ReadWriteRequest>,
		worker_event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	) {
		let semaphore = Arc::new(tokio::sync::Semaphore::new(READ_WRITE_ASYNC_CONCURRENCY));
		let mut tasks = JoinSet::new();

		// Dispatch requests while the input channel is open.
		loop {
			tokio::select! {
				request = receiver.recv() => {
					let Some(request) = request else {
						break;
					};
					let permit = semaphore.clone().acquire_owned().await.unwrap();
					let server = self.clone();
					tasks.spawn(async move {
						let _permit = permit;
						server.handle_read_write_request(request).await
					});
				},
				result = tasks.join_next(), if !tasks.is_empty() => {
					if let Some(result) = result {
						Self::handle_read_write_task_result(result, &worker_event_sender);
					}
				},
			}
		}

		// Cancel and drain the remaining tasks.
		tasks.abort_all();
		while let Some(result) = tasks.join_next().await {
			if result
				.as_ref()
				.is_err_and(tokio::task::JoinError::is_cancelled)
			{
				continue;
			}
			Self::handle_read_write_task_result(result, &worker_event_sender);
		}
	}

	async fn handle_read_write_request(&self, request: ReadWriteRequest) -> Result<()> {
		let ReadWriteRequest { fd, request, token } = request;
		let unique = request.header.unique;
		let opcode = request.header.opcode;
		let result = self
			.handle_cancellable_request(fd.as_ref(), request, token)
			.await;
		self.complete_read_write_response(fd.as_ref(), unique, opcode, result)
	}

	fn complete_read_write_response(
		&self,
		fd: &OwnedFd,
		unique: u64,
		opcode: sys::fuse_opcode,
		result: Result<Response>,
	) -> Result<()> {
		// Log the unexpected request errors.
		if let Err(error) = &result {
			let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
			if !Self::is_expected_error(opcode, error_code) {
				tracing::error!(?error, ?opcode, "unexpected error");
			}
		}

		// Write the response and finalize its resources.
		if let Err(error) = Self::write_response(fd, unique, result) {
			self.rollback_response_resources(fd, unique);
			match error.raw_os_error() {
				Some(libc::ENOENT | libc::EINTR | libc::EAGAIN) => {},
				_ => return Err(error),
			}
		} else {
			self.commit_response_resources(unique);
		}

		Ok(())
	}

	fn handle_read_write_task_result(
		result: std::result::Result<Result<()>, tokio::task::JoinError>,
		worker_event_sender: &tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	) {
		let error = match result {
			Err(error) => Error::other(format!("a ReadWrite request task failed: {error}")),
			Ok(Err(error)) => error,
			Ok(Ok(())) => return,
		};
		tracing::error!(%error, "a ReadWrite request task failed");
		worker_event_sender
			.send(WorkerEvent::Failed {
				error,
				worker: "ReadWrite request dispatcher".to_owned(),
			})
			.ok();
	}

	pub(super) fn thread_loop_read_write(
		&self,
		fd: &Arc<OwnedFd>,
		request_buffer_size: usize,
		request_sender: &tokio::sync::mpsc::Sender<ReadWriteRequest>,
	) -> Result<()> {
		let mut buffer = vec![0u8; request_buffer_size];
		let mut batch_results = Vec::<Dispatch>::with_capacity(1);
		loop {
			// Read and parse a request.
			let size = loop {
				match rustix::io::read(fd.as_ref(), &mut buffer) {
					Err(Errno::INTR) => {},
					Err(Errno::NOTCONN) => return Ok(()),
					Err(error) => return Err(error.into()),
					Ok(size) => break size,
				}
			};
			if size == 0 {
				return Ok(());
			}
			let size = size.to_usize().unwrap();
			let request = Self::deserialize_request(&buffer[..size])?;
			let unique = request.header.unique;
			let opcode = request.header.opcode;

			// Try the synchronous fast path.
			let pending_request = PendingRequest {
				request: request.clone(),
				slot: 0,
			};
			self.handle_request_sync_batch(fd.as_ref(), &[pending_request], &mut batch_results)?;
			let dispatch = batch_results
				.pop()
				.ok_or_else(|| Error::other("missing sync batch result"))?;

			// Queue a deferred request.
			let Dispatch::Ready(result) = dispatch else {
				let token = self.register_async_request(unique)?;
				let request = ReadWriteRequest {
					fd: fd.clone(),
					request,
					token,
				};
				if let Err(error) = request_sender.blocking_send(request) {
					self.finish_async_request(error.0.request.header.unique);
					return Err(Error::other("the ReadWrite request dispatcher stopped"));
				}
				continue;
			};
			self.complete_read_write_response(fd.as_ref(), unique, opcode, result)?;
		}
	}
}
