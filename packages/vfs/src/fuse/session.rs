use super::*;

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
		unique: u64,
		result: &Result<Option<Response>>,
	) -> Result<()> {
		let nodes = match result {
			Ok(Some(Response::Lookup(response))) => vec![response.nodeid],
			Ok(Some(Response::ReadDirPlus { nodes, .. })) => nodes.clone(),
			_ => Vec::new(),
		};
		let handle = match result {
			Ok(Some(Response::Open(response) | Response::OpenDir(response))) => Some(response.fh),
			_ => None,
		};
		if nodes.is_empty() && handle.is_none() {
			return Ok(());
		}
		if !nodes.is_empty() {
			let mut publications = self.pending_publications.lock().unwrap();
			if publications.contains_key(&unique) {
				drop(publications);
				self.rollback_nodes(nodes);
				return Err(Error::other(format!(
					"a FUSE response with unique ID {unique} already has pending publications",
				)));
			}
			publications.insert(unique, nodes);
		}
		if let Some(handle) = handle {
			let mut handles = self.pending_handles.lock().unwrap();
			if handles.contains_key(&unique) {
				drop(handles);
				self.provider.close_sync(handle);
				return Err(Error::other(format!(
					"a FUSE response with unique ID {unique} already has a pending handle",
				)));
			}
			handles.insert(unique, handle);
		}

		Ok(())
	}

	pub(super) fn commit_response_resources(&self, unique: u64) {
		self.pending_handles.lock().unwrap().remove(&unique);
		self.pending_publications.lock().unwrap().remove(&unique);
	}

	pub(super) fn rollback_response_resources(&self, fd: &OwnedFd, unique: u64) {
		let handle = self.pending_handles.lock().unwrap().remove(&unique);
		if let Some(handle) = handle {
			self.close_passthrough_backing(fd, handle);
			self.provider.close_sync(handle);
		}
		self.rollback_response_publications(unique);
	}

	pub(super) fn rollback_response_publications(&self, unique: u64) {
		let nodes = self.pending_publications.lock().unwrap().remove(&unique);
		if let Some(nodes) = nodes {
			self.rollback_nodes(nodes);
		}
	}

	pub(super) fn rollback_all_response_resources(&self, fd: &OwnedFd) {
		let handles = std::mem::take(&mut *self.pending_handles.lock().unwrap());
		for handle in handles.into_values() {
			self.close_passthrough_backing(fd, handle);
			self.provider.close_sync(handle);
		}
		let publications = std::mem::take(&mut *self.pending_publications.lock().unwrap());
		for nodes in publications.into_values() {
			self.rollback_nodes(nodes);
		}
	}

	pub(super) fn rollback_nodes(&self, nodes: Vec<u64>) {
		for node in nodes {
			self.provider.forget_sync(node, 1);
		}
	}

	pub(super) async fn handle_cancellable_request(
		&self,
		fd: &OwnedFd,
		request: Request,
		token: CancellationToken,
	) -> Result<Option<Response>> {
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
			let size = loop {
				match rustix::io::read(fd.as_ref(), &mut buffer) {
					Ok(size) => break size,
					Err(Errno::INTR) => {},
					Err(Errno::NODEV | Errno::NOTCONN) => return Ok(()),
					Err(error) => return Err(error.into()),
				}
			};
			if size == 0 {
				return Ok(());
			}
			let size = size.to_usize().unwrap();
			let request = Self::deserialize_request(&buffer[..size])?;
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
		result: Result<Option<Response>>,
	) -> Result<()> {
		let (error, response) = match result {
			Ok(Some(response)) => {
				if !Self::requires_response(&response) {
					return Ok(());
				}
				(0, Some(response))
			},
			Ok(None) => (0, None),
			Err(error) => (error.raw_os_error().unwrap_or(libc::ENOSYS), None),
		};
		let payload = response.as_ref().map_or(&[][..], Self::response_bytes);
		let len = size_of::<fuse_out_header>() + payload.len();
		let header = fuse_out_header {
			unique,
			len: len.to_u32().unwrap(),
			error: -error,
		};
		let iov = [IoSlice::new(header.as_bytes()), IoSlice::new(payload)];
		let written = loop {
			match rustix::io::writev(fd, &iov) {
				Ok(written) => break written,
				Err(Errno::INTR) => {},
				Err(error) => return Err(error.into()),
			}
		};
		if written != len {
			return Err(Error::other("failed to write a complete response"));
		}
		Ok(())
	}

	pub(super) fn requires_response(response: &Response) -> bool {
		!matches!(
			response,
			Response::BatchForget | Response::Forget | Response::Interrupt
		)
	}

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
			Response::Lookup(data) => data.as_bytes(),
			Response::Open(data) | Response::OpenDir(data) => data.as_bytes(),
			Response::Read(data) => data.as_ref(),
			Response::ReadDir(data) | Response::GetXattr(data) | Response::ListXattr(data) => {
				data.as_bytes()
			},
			Response::ReadDirPlus { data, .. } => data.as_bytes(),
			Response::ReadLink(data) => data.as_bytes(),
			Response::Statfs(data) => data.as_bytes(),
			Response::Statx(data) => data.as_bytes(),
		}
	}

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

fn signal_eventfd(fd: &OwnedFd) -> Result<()> {
	let value = 1u64.to_ne_bytes();
	let ret = loop {
		match rustix::io::write(fd, &value) {
			Ok(ret) => break ret,
			Err(Errno::INTR) => {},
			Err(error) => return Err(error.into()),
		}
	};
	if ret != value.len() {
		return Err(Error::other("failed to signal eventfd"));
	}
	Ok(())
}
