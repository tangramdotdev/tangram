use {super::*, tokio::task::JoinSet};

pub(super) struct ReadWriteRequest {
	pub(super) fd: Arc<OwnedFd>,
	pub(super) request: Request,
	pub(super) token: CancellationToken,
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn spawn_read_write_dispatcher(
		&self,
		mut receiver: tokio::sync::mpsc::Receiver<ReadWriteRequest>,
		worker_event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	) -> tokio::task::JoinHandle<()> {
		let semaphore = Arc::new(tokio::sync::Semaphore::new(READ_WRITE_ASYNC_CONCURRENCY));
		let server = self.clone();
		tokio::spawn(async move {
			let mut tasks = JoinSet::new();
			loop {
				tokio::select! {
					request = receiver.recv() => {
						let Some(request) = request else {
							break;
						};
						let permit = semaphore.clone().acquire_owned().await.unwrap();
						let server = server.clone();
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
		})
	}

	fn handle_read_write_task_result(
		result: std::result::Result<Result<()>, tokio::task::JoinError>,
		worker_event_sender: &tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	) {
		let error = match result {
			Ok(Ok(())) => return,
			Ok(Err(error)) => error,
			Err(error) => Error::other(format!("a ReadWrite request task failed: {error}")),
		};
		tracing::error!(%error, "a ReadWrite request task failed");
		worker_event_sender
			.send(WorkerEvent::Failed {
				error,
				worker: "ReadWrite request dispatcher".to_owned(),
			})
			.ok();
	}

	async fn handle_read_write_request(&self, request: ReadWriteRequest) -> Result<()> {
		let ReadWriteRequest { fd, request, token } = request;
		let unique = request.header.unique;
		let opcode = request.header.opcode;
		let result = self
			.handle_cancellable_request(fd.as_ref(), request, token)
			.await;
		if let Err(error) = &result {
			let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
			if !Self::is_expected_error(opcode, error_code) {
				tracing::error!(?error, ?opcode, "unexpected error");
			}
		}
		if let Err(error) = Self::write_response(fd.as_ref(), unique, result) {
			self.rollback_response_resources(fd.as_ref(), unique);
			match error.raw_os_error() {
				Some(libc::ENOENT | libc::EINTR | libc::EAGAIN) => {},
				_ => return Err(error),
			}
		} else {
			self.commit_response_resources(unique);
		}

		Ok(())
	}

	pub(super) fn thread_loop_read_write(
		&self,
		fd: &Arc<OwnedFd>,
		request_buffer_size: usize,
		request_sender: &tokio::sync::mpsc::Sender<ReadWriteRequest>,
	) -> Result<()> {
		let mut buffer = vec![0u8; request_buffer_size];
		let mut batch_results = Vec::<Result<Option<Response>>>::with_capacity(1);
		loop {
			let size = loop {
				match rustix::io::read(fd.as_ref(), &mut buffer) {
					Ok(size) => break size,
					Err(Errno::INTR) => {},
					Err(Errno::NOTCONN) => return Ok(()),
					Err(error) => return Err(error.into()),
				}
			};
			if size == 0 {
				return Ok(());
			}
			let size = size.to_usize().unwrap();
			let request = Self::deserialize_request(&buffer[..size])?;
			let unique = request.header.unique;
			let opcode = request.header.opcode;
			let pending_request = PendingRequest {
				slot: 0,
				request: request.clone(),
			};
			self.handle_request_sync_batch(fd.as_ref(), &[pending_request], &mut batch_results)?;
			let result = batch_results
				.pop()
				.ok_or_else(|| Error::other("missing sync batch result"))?;
			let deferred = match &result {
				Ok(None) => true,
				Err(error)
					if error.raw_os_error() == Some(libc::ENOSYS)
						&& opcode != sys::fuse_opcode_FUSE_OPENDIR =>
				{
					true
				},
				_ => false,
			};
			if deferred {
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
			}
			if let Err(error) = &result {
				let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
				if !Self::is_expected_error(opcode, error_code) {
					tracing::error!(?error, ?opcode, "unexpected error");
				}
			}
			if let Err(error) = Self::write_response(fd.as_ref(), unique, result) {
				self.rollback_response_resources(fd.as_ref(), unique);
				match error.raw_os_error() {
					Some(libc::ENOENT | libc::EINTR | libc::EAGAIN) => {},
					_ => return Err(error),
				}
			} else {
				self.commit_response_resources(unique);
			}
		}
	}
}
