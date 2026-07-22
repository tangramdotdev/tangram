use super::*;

pub(super) fn notify_async_response(
	async_notification_pending: &AtomicBool,
	eventfd: &OwnedFd,
	response: AsyncResponse,
	sender: &crossbeam_channel::Sender<AsyncResponse>,
	worker_event_sender: &tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	worker_id: usize,
) {
	if sender.send(response).is_err() || async_notification_pending.swap(true, Ordering::AcqRel) {
		return;
	}
	if let Err(error) = signal_eventfd(eventfd) {
		async_notification_pending.store(false, Ordering::Release);
		tracing::error!(?error, %worker_id, "failed to signal the io_uring eventfd");
		worker_event_sender
			.send(WorkerEvent::Failed {
				error,
				worker: format!("io_uring worker {worker_id} async notifier"),
			})
			.ok();
	}
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn register_async_request(&self, unique: u64) -> Result<CancellationToken> {
		let token = CancellationToken::new();
		let mut requests = self.active_requests.lock().unwrap();
		if requests.contains_key(&unique) {
			return Err(Error::other(format!(
				"a FUSE request with unique ID {unique} is already active",
			)));
		}
		requests.insert(unique, token.clone());

		Ok(token)
	}

	pub(super) fn cancel_async_request(&self, unique: u64) -> bool {
		let token = self.active_requests.lock().unwrap().get(&unique).cloned();
		if let Some(token) = token {
			token.cancel();
			return true;
		}

		false
	}

	pub(super) fn cancel_async_requests(&self) {
		let requests = std::mem::take(&mut *self.active_requests.lock().unwrap());
		for token in requests.into_values() {
			token.cancel();
		}
	}

	pub(super) fn finish_async_request(&self, unique: u64) {
		self.active_requests.lock().unwrap().remove(&unique);
	}

	pub(super) fn register_response_resources(
		&self,
		fd: &OwnedFd,
		unique: u64,
		result: &Result<Response>,
	) -> Result<()> {
		// Collect resources transferred by the response.
		let resources = match result {
			Ok(Response::Lookup(response)) => ResponseResources {
				nodes: vec![response.nodeid],
				..ResponseResources::default()
			},
			Ok(Response::Open(response) | Response::OpenDir(response)) => ResponseResources {
				handle: Some(response.fh),
				..ResponseResources::default()
			},
			Ok(Response::ReadDirPlus { nodes, .. }) => ResponseResources {
				nodes: nodes.clone(),
				..ResponseResources::default()
			},
			_ => ResponseResources::default(),
		};
		if resources.handle.is_none() && resources.nodes.is_empty() {
			return Ok(());
		}

		// Register the resources until the response is committed.
		let mut pending = self.pending_response_resources.lock().unwrap();
		if pending.contains_key(&unique) {
			drop(pending);
			self.rollback_resources(fd, resources);
			return Err(Error::other(format!(
				"a FUSE response with unique ID {unique} already has pending resources",
			)));
		}
		pending.insert(unique, resources);

		Ok(())
	}

	pub(super) fn commit_response_resources(&self, unique: u64) {
		self.pending_response_resources
			.lock()
			.unwrap()
			.remove(&unique);
	}

	pub(super) fn rollback_response_resources(&self, fd: &OwnedFd, unique: u64) {
		let resources = self
			.pending_response_resources
			.lock()
			.unwrap()
			.remove(&unique);
		if let Some(resources) = resources {
			self.rollback_resources(fd, resources);
		}
	}

	pub(super) fn rollback_all_response_resources(&self, fd: &OwnedFd) {
		let resources = std::mem::take(&mut *self.pending_response_resources.lock().unwrap());
		for resources in resources.into_values() {
			self.rollback_resources(fd, resources);
		}
	}

	fn rollback_resources(&self, fd: &OwnedFd, resources: ResponseResources) {
		if let Some(handle) = resources.handle {
			self.close_passthrough_backing(fd, handle);
			self.provider.close_sync(handle);
		}
		for node in resources.nodes {
			self.provider.forget_sync(node, 1);
		}
	}

	pub(super) async fn handle_cancellable_request(
		&self,
		fd: &OwnedFd,
		request: Request,
		token: CancellationToken,
	) -> Result<Response> {
		let unique = request.header.unique;
		let result = tokio::select! {
			biased;
			() = token.cancelled() => Err(Error::from_raw_os_error(libc::EINTR)),
			result = self.handle_request(fd, request) => result,
		};
		self.finish_async_request(unique);

		result
	}

	pub(super) fn thread_loop_control(
		&self,
		fd: &Arc<OwnedFd>,
		request_buffer_size: usize,
	) -> Result<()> {
		let mut buffer = vec![0u8; request_buffer_size];
		loop {
			// Read and parse a control request.
			let size = loop {
				match rustix::io::read(fd.as_ref(), &mut buffer) {
					Err(Errno::INTR) => {},
					Err(Errno::NODEV | Errno::NOTCONN) => return Ok(()),
					Err(error) => return Err(error.into()),
					Ok(size) => break size,
				}
			};
			if size == 0 {
				return Ok(());
			}
			let size = size.to_usize().unwrap();
			let request = Self::deserialize_request(&buffer[..size])?;

			// Dispatch the control request.
			match &request.data {
				RequestData::BatchForget(data, entries) => {
					let count = data.count.to_usize().unwrap_or(0);
					for entry in entries.iter().take(count) {
						self.provider.forget_sync(entry.nodeid, entry.nlookup);
					}
				},
				RequestData::Forget(data) => {
					self.provider
						.forget_sync(request.header.nodeid, data.nlookup);
				},
				RequestData::Interrupt(data) => {
					let result = self.handle_interrupt_request(*data);
					if let Err(error) =
						Self::write_response(fd.as_ref(), request.header.unique, result)
					{
						match error.raw_os_error() {
							Some(libc::EAGAIN | libc::EINTR | libc::ENOENT) => {},
							_ => return Err(error),
						}
					}
				},
				_ => {
					return Err(Error::other(format!(
						"received opcode {} on the io_uring control channel",
						request.header.opcode,
					)));
				},
			}
		}
	}

	pub(super) fn write_response(
		fd: &OwnedFd,
		unique: u64,
		result: Result<Response>,
	) -> Result<()> {
		// Encode the response.
		let (error, response) = match result {
			Err(error) => (error.raw_os_error().unwrap_or(libc::ENOSYS), None),
			Ok(response) => {
				if !Self::requires_response(&response) {
					return Ok(());
				}
				(0, Some(response))
			},
		};
		let payload = response.as_ref().map_or(&[][..], Self::response_bytes);
		let len = size_of::<fuse_out_header>() + payload.len();
		let header = fuse_out_header {
			error: -error,
			len: len.to_u32().unwrap(),
			unique,
		};
		let iov = [IoSlice::new(header.as_bytes()), IoSlice::new(payload)];

		// Write the complete response.
		let written = loop {
			match rustix::io::writev(fd, &iov) {
				Err(Errno::INTR) => {},
				Err(error) => return Err(error.into()),
				Ok(written) => break written,
			}
		};
		if written != len {
			return Err(Error::other("failed to write a complete response"));
		}
		Ok(())
	}

	#[must_use]
	pub(super) fn requires_response(response: &Response) -> bool {
		!matches!(
			response,
			Response::BatchForget | Response::Forget | Response::Interrupt
		)
	}

	#[must_use]
	pub(super) fn response_bytes(response: &Response) -> &[u8] {
		debug_assert!(Self::requires_response(response));
		match response {
			Response::BatchForget
			| Response::Destroy
			| Response::Flush
			| Response::Forget
			| Response::Interrupt
			| Response::Release
			| Response::ReleaseDir => &[],
			Response::GetAttr(data) => data.as_bytes(),
			Response::GetXattr(data)
			| Response::ListXattr(data)
			| Response::ReadDir(data)
			| Response::ReadDirPlus { data, .. } => data.as_bytes(),
			Response::Lookup(data) => data.as_bytes(),
			Response::Open(data) | Response::OpenDir(data) => data.as_bytes(),
			Response::Read(data) => data.as_ref(),
			Response::ReadLink(data) => data.as_bytes(),
			Response::Statfs(data) => data.as_bytes(),
			Response::Statx(data) => data.as_bytes(),
		}
	}

	#[must_use]
	pub(super) fn is_expected_error(opcode: sys::fuse_opcode, error_code: i32) -> bool {
		(opcode == sys::fuse_opcode_FUSE_LOOKUP && error_code == libc::ENOENT)
			|| (opcode == sys::fuse_opcode_FUSE_GETXATTR
				&& (error_code == libc::ENODATA || error_code == libc::ERANGE))
			|| (opcode == sys::fuse_opcode_FUSE_LISTXATTR && error_code == libc::ERANGE)
			|| (matches!(
				opcode,
				sys::fuse_opcode_FUSE_OPEN | sys::fuse_opcode_FUSE_OPENDIR
			) && error_code == libc::EROFS)
			|| (opcode == sys::fuse_opcode_FUSE_INTERRUPT && error_code == libc::EAGAIN)
			|| matches!(error_code, libc::EINTR | libc::ENOSYS)
	}
}

fn signal_eventfd(fd: &OwnedFd) -> Result<()> {
	let value = 1u64.to_ne_bytes();
	let ret = loop {
		match rustix::io::write(fd, &value) {
			Err(Errno::INTR) => {},
			Err(error) => return Err(error.into()),
			Ok(ret) => break ret,
		}
	};
	if ret != value.len() {
		return Err(Error::other("failed to signal eventfd"));
	}
	Ok(())
}
