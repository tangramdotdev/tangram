use super::*;

const EVENTFD_USER_DATA: u64 = u64::MAX;
const IO_URING_MAX_RETIRED_SLOTS_PER_WORKER: usize = 8;
const THREAD_CQE_BATCH_SIZE: usize = 64;
const THREAD_FIXED_EVENTFD_FD: u32 = 1;
const THREAD_FIXED_FUSE_FD: u32 = 0;
const URING_CMD_BYTES: usize = 80;
const URING_IOVEC_COUNT: u32 = 2;

// The field order matches the kernel's stable io_uring submission entry prefix.
#[repr(C)]
struct IoUringSqePrefix {
	opcode: u8,
	flags: u8,
	ioprio: u16,
	fd: i32,
	off: u64,
	addr: u64,
	len: u32,
	rw_flags: u32,
	user_data: u64,
	buf_index_or_group: u16,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::fuse) enum UringSlotState {
	Async {
		opcode: sys::fuse_opcode,
		unique: u64,
	},
	Commit {
		unique: u64,
	},
	Register,
	Request {
		opcode: sys::fuse_opcode,
		unique: u64,
	},
}

pub(in crate::fuse) struct UringSlot {
	pub(in crate::fuse) header: Box<sys::fuse_uring_req_header>,
	iovecs: [libc::iovec; URING_IOVEC_COUNT as usize],
	pub(in crate::fuse) payload: Vec<u8>,
	pub(in crate::fuse) qid: u16,
	pub(in crate::fuse) state: UringSlotState,
}

struct RingWorker<P> {
	async_notification_pending: Arc<AtomicBool>,
	event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	eventfd: Arc<OwnedFd>,
	eventfd_inflight: bool,
	fd: Arc<OwnedFd>,
	io_uring: IoUring<io_uring::squeue::Entry128>,
	payload_size: usize,
	receiver: crossbeam_channel::Receiver<AsyncResponse>,
	retired_slots: VecDeque<Box<UringSlot>>,
	ring_eventfd_buffer: Box<[u8; size_of::<u64>()]>,
	runtime: tokio::runtime::Handle,
	sender: crossbeam_channel::Sender<AsyncResponse>,
	server: Server<P>,
	slots: Box<[Box<UringSlot>]>,
	worker_id: usize,
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn thread_loop(
		&self,
		fd: &Arc<OwnedFd>,
		runtime: &tokio::runtime::Handle,
		config: RingWorkerConfig,
	) -> Result<()> {
		RingWorker::new(self.clone(), fd.clone(), runtime.clone(), config)?.run()
	}

	#[must_use]
	fn build_fuse_uring_cmd_entry(
		fd: types::Fixed,
		cmd_op: u32,
		commit_id: u64,
		qid: u16,
		iovecs: *const libc::iovec,
		user_data: u64,
		command_flags: u64,
	) -> io_uring::squeue::Entry128 {
		let request = sys::fuse_uring_cmd_req {
			commit_id,
			flags: command_flags,
			padding: [0; 6],
			qid,
		};
		let mut cmd = [0u8; URING_CMD_BYTES];
		cmd[..size_of::<sys::fuse_uring_cmd_req>()].copy_from_slice(request.as_bytes());
		let mut entry = opcode::UringCmd80::new(fd, cmd_op)
			.cmd(cmd)
			.build()
			.user_data(user_data);
		Self::configure_fuse_uring_entry_iovec(&mut entry, iovecs);
		entry
	}

	fn configure_fuse_uring_entry_iovec(
		entry: &mut io_uring::squeue::Entry128,
		iovecs: *const libc::iovec,
	) {
		let sqe = std::ptr::from_mut(entry).cast::<IoUringSqePrefix>();
		// SAFETY: `Entry128` begins with the kernel's stable `io_uring_sqe` prefix, and the
		// pointer is valid and exclusively borrowed for these field writes.
		unsafe {
			(*sqe).addr = iovecs as u64;
			(*sqe).len = URING_IOVEC_COUNT;
		}
	}

	fn submit_register(
		io_uring: &mut IoUring<io_uring::squeue::Entry128>,
		fixed_fuse_fd: types::Fixed,
		slot: usize,
		slot_data: &mut UringSlot,
	) -> Result<()> {
		if !matches!(slot_data.state, UringSlotState::Register) {
			return Err(Error::other("attempted to register a busy io_uring slot"));
		}
		let entry = Self::build_fuse_uring_cmd_entry(
			fixed_fuse_fd,
			sys::fuse_uring_cmd_FUSE_IO_URING_CMD_REGISTER,
			0,
			slot_data.qid,
			slot_data.iovecs.as_ptr(),
			slot.to_u64().unwrap(),
			0,
		);
		let mut submission = io_uring.submission();
		// SAFETY: The entry references a boxed slot that remains allocated until the ring is
		// destroyed or is retained in the retired-slot queue.
		if unsafe { submission.push(&entry) }.is_err() {
			return Err(Error::other(
				"failed to submit an io_uring register command",
			));
		}

		Ok(())
	}

	pub(in crate::fuse) fn replace_stale_uring_slot(
		slots: &mut [Box<UringSlot>],
		retired_slots: &mut VecDeque<Box<UringSlot>>,
		payload_size: usize,
		slot: usize,
	) -> Result<()> {
		if retired_slots.len() >= IO_URING_MAX_RETIRED_SLOTS_PER_WORKER {
			return Err(Error::other(
				"the io_uring stale slot retirement limit was exhausted",
			));
		}
		let slot_data = slots
			.get_mut(slot)
			.ok_or_else(|| Error::other(format!("invalid stale io_uring slot {slot}")))?;
		if !matches!(slot_data.state, UringSlotState::Commit { .. }) {
			return Err(Error::other(format!(
				"attempted to replace io_uring slot {slot} in state {:?}",
				slot_data.state,
			)));
		}
		let qid = slot_data.qid;
		let replacement = Box::new(UringSlot::new(qid, payload_size));
		let retired = std::mem::replace(slot_data, replacement);
		retired_slots.push_back(retired);
		tracing::debug!(%slot, %qid, "replaced a stale io_uring slot");

		Ok(())
	}

	fn submit_commit_and_fetch(
		&self,
		io_uring: &mut IoUring<io_uring::squeue::Entry128>,
		response_fd: &OwnedFd,
		slot: usize,
		slot_data: &mut UringSlot,
		unique: u64,
		result: Result<Response>,
	) -> Result<()> {
		let (UringSlotState::Async {
			unique: expected_unique,
			..
		}
		| UringSlotState::Request {
			unique: expected_unique,
			..
		}) = slot_data.state
		else {
			return Err(Error::other("attempted to commit an idle io_uring slot"));
		};
		if expected_unique != unique {
			return Err(Error::other(
				"attempted to commit the wrong io_uring request",
			));
		}
		if let Err(error) = Self::prepare_uring_response(slot_data, unique, result) {
			self.rollback_response_resources(response_fd, unique);
			return Err(error);
		}
		let entry = Self::build_fuse_uring_cmd_entry(
			types::Fixed(THREAD_FIXED_FUSE_FD),
			sys::fuse_uring_cmd_FUSE_IO_URING_CMD_COMMIT_AND_FETCH,
			unique,
			slot_data.qid,
			slot_data.iovecs.as_ptr(),
			slot.to_u64().unwrap(),
			0,
		);
		let mut submission = io_uring.submission();
		// SAFETY: The entry references `slot_data`, which remains boxed and allocated through
		// completion or is retained in the retired-slot queue.
		if unsafe { submission.push(&entry) }.is_err() {
			self.rollback_response_resources(response_fd, unique);
			return Err(Error::other(
				"failed to submit uring commit and fetch command",
			));
		}
		slot_data.state = UringSlotState::Commit { unique };
		Ok(())
	}

	fn prepare_uring_response(
		slot_data: &mut UringSlot,
		unique: u64,
		result: Result<Response>,
	) -> Result<()> {
		let in_out = slot_data.header.in_out.as_mut_bytes();
		in_out.fill(0);
		let (error, payload_len, needs_header) = match result {
			Err(error) => (error.raw_os_error().unwrap_or(libc::ENOSYS), 0, true),
			Ok(response) => {
				if Self::requires_response(&response) {
					let payload = Self::response_bytes(&response);
					if payload.len() > slot_data.payload.len() {
						return Err(Error::other("response payload too large"));
					}
					slot_data.payload[..payload.len()].copy_from_slice(payload);
					(0, payload.len(), true)
				} else {
					(0, 0, false)
				}
			},
		};
		if needs_header {
			let len = size_of::<fuse_out_header>() + payload_len;
			let out = fuse_out_header {
				error: -error,
				len: len.to_u32().unwrap(),
				unique,
			};
			in_out[..size_of::<fuse_out_header>()].copy_from_slice(out.as_bytes());
		}
		slot_data.header.ring_ent_in_out = sys::fuse_uring_ent_in_out {
			commit_id: unique,
			flags: 0,
			padding: 0,
			payload_sz: payload_len.to_u32().unwrap(),
			reserved: 0,
		};
		Ok(())
	}

	fn spawn_async_request(&self, context: AsyncRequestContext) {
		let AsyncRequestContext {
			async_notification_pending,
			eventfd,
			fd,
			request,
			runtime,
			sender,
			slot,
			token,
			worker_event_sender,
			worker_id,
		} = context;
		let server = self.clone();
		runtime.spawn(async move {
			let opcode = request.header.opcode;
			let unique = request.header.unique;
			let result = server
				.handle_cancellable_request(fd.as_ref(), request, token)
				.await;
			let response = AsyncResponse {
				opcode,
				result,
				slot,
				unique,
			};
			notify_async_response(
				&async_notification_pending,
				eventfd.as_ref(),
				response,
				&sender,
				&worker_event_sender,
				worker_id,
			);
		});
	}

	fn drain_async_responses(
		&self,
		worker_id: usize,
		response_fd: &OwnedFd,
		io_uring: &mut IoUring<io_uring::squeue::Entry128>,
		slots: &mut [Box<UringSlot>],
		receiver: &crossbeam_channel::Receiver<AsyncResponse>,
	) -> Result<()> {
		while let Ok(response) = receiver.try_recv() {
			let Some(slot_data) = slots.get_mut(response.slot) else {
				return Err(Error::other(format!(
					"received an async response for invalid io_uring slot {}",
					response.slot,
				)));
			};
			let UringSlotState::Async { opcode, unique } = slot_data.state else {
				return Err(Error::other(format!(
					"received an async response for io_uring slot {} in state {:?}",
					response.slot, slot_data.state,
				)));
			};
			if unique != response.unique || opcode != response.opcode {
				return Err(Error::other(format!(
					"received an async response for a different request in io_uring slot {}",
					response.slot,
				)));
			}
			if let Err(error) = &response.result {
				let code = error.raw_os_error().unwrap_or(libc::ENOSYS);
				if !Self::is_expected_error(response.opcode, code) {
					tracing::error!(?error, opcode = response.opcode, %worker_id, "unexpected error");
				}
			}
			self.submit_commit_and_fetch(
				io_uring,
				response_fd,
				response.slot,
				slot_data,
				response.unique,
				response.result,
			)?;
		}
		Ok(())
	}
}

impl<P> RingWorker<P>
where
	P: Provider + Send + Sync + 'static,
{
	fn new(
		server: Server<P>,
		fd: Arc<OwnedFd>,
		runtime: tokio::runtime::Handle,
		config: RingWorkerConfig,
	) -> Result<Self> {
		let RingWorkerConfig {
			event_sender,
			payload_size,
			queue_ids,
			slots_per_queue,
			sqpoll_wq_fd,
			worker_id,
		} = config;
		let eventfd = rustix::event::eventfd(0, EventfdFlags::CLOEXEC)
			.map(Arc::new)
			.map_err(|error| {
				Error::other(format!("failed to create the thread eventfd: {error}"))
			})?;
		let (sender, receiver) = crossbeam_channel::unbounded::<AsyncResponse>();
		let slot_count = queue_ids
			.len()
			.checked_mul(slots_per_queue)
			.ok_or_else(|| Error::other("the io_uring slot count overflowed"))?;
		let ring_entries = slot_count
			.checked_add(2)
			.ok_or_else(|| Error::other("the io_uring entry count overflowed"))?
			.checked_next_power_of_two()
			.ok_or_else(|| Error::other("the io_uring entry count overflowed"))?
			.max(32)
			.to_u32()
			.ok_or_else(|| Error::other("the io_uring entry count is out of range"))?;
		let ring_eventfd_buffer = Box::new([0u8; size_of::<u64>()]);
		let slots = queue_ids
			.iter()
			.flat_map(|&qid| {
				(0..slots_per_queue).map(move |_| Box::new(UringSlot::new(qid, payload_size)))
			})
			.collect::<Vec<_>>()
			.into_boxed_slice();
		let mut builder = IoUring::<io_uring::squeue::Entry128>::builder();
		builder.setup_attach_wq(sqpoll_wq_fd).setup_no_sqarray();
		let io_uring = builder
			.build(ring_entries)
			.map_err(|error| Error::other(format!("failed to build the thread ring: {error}")))?;
		io_uring
			.submitter()
			.register_files(&[fd.as_raw_fd(), eventfd.as_raw_fd()])
			.map_err(|error| {
				Error::other(format!("failed to register the thread files: {error}"))
			})?;
		Ok(Self {
			async_notification_pending: Arc::new(AtomicBool::new(false)),
			event_sender,
			eventfd,
			eventfd_inflight: false,
			fd,
			io_uring,
			payload_size,
			receiver,
			retired_slots: VecDeque::new(),
			ring_eventfd_buffer,
			runtime,
			sender,
			server,
			slots,
			worker_id,
		})
	}

	fn run(mut self) -> Result<()> {
		let mut batch_requests = Vec::<PendingRequest>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut batch_results = Vec::<Dispatch>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut commits =
			Vec::<(usize, u64, Result<Response>)>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut deferred = Vec::<PendingRequest>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut register_retries = Vec::<usize>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut stale_commits = Vec::<usize>::with_capacity(THREAD_CQE_BATCH_SIZE);
		{
			let mut submission = self.io_uring.submission();
			for (slot, slot_data) in self.slots.iter().enumerate() {
				let entry = Server::<P>::build_fuse_uring_cmd_entry(
					types::Fixed(THREAD_FIXED_FUSE_FD),
					sys::fuse_uring_cmd_FUSE_IO_URING_CMD_REGISTER,
					0,
					slot_data.qid,
					slot_data.iovecs.as_ptr(),
					slot.to_u64().unwrap(),
					0,
				);
				// SAFETY: Each entry references a slot owned by the worker until the ring is dropped.
				if unsafe { submission.push(&entry) }.is_err() {
					return Err(Error::other("failed to submit uring register command"));
				}
			}
		}
		self.io_uring.submit().map_err(|error| {
			Error::other(format!("failed to submit register commands: {error}"))
		})?;
		self.event_sender.send(WorkerEvent::Ready).ok();

		loop {
			{
				let mut submission = self.io_uring.submission();
				if !self.eventfd_inflight {
					let entry: io_uring::squeue::Entry128 = opcode::Read::new(
						types::Fixed(THREAD_FIXED_EVENTFD_FD),
						self.ring_eventfd_buffer.as_mut_ptr(),
						self.ring_eventfd_buffer.len().to_u32().unwrap(),
					)
					.offset(u64::MAX)
					.build()
					.user_data(EVENTFD_USER_DATA)
					.into();
					// SAFETY: The eventfd buffer remains allocated until the ring is destroyed.
					if unsafe { submission.push(&entry) }.is_ok() {
						self.eventfd_inflight = true;
					}
				}
			}

			if let Err(error) = self.io_uring.submit_and_wait(1) {
				match error.raw_os_error() {
					Some(libc::EINTR) => {
						continue;
					},
					Some(libc::ENODEV) => {
						return Ok(());
					},
					_ => {
						return Err(Error::other(format!(
							"failed to submit the thread ring and wait: {error}",
						)));
					},
				}
			}

			let mut saw_eventfd = false;
			batch_requests.clear();
			register_retries.clear();
			stale_commits.clear();
			{
				let mut completion = self.io_uring.completion();
				for completion in completion.by_ref().take(THREAD_CQE_BATCH_SIZE) {
					if completion.user_data() == EVENTFD_USER_DATA {
						self.eventfd_inflight = false;
						saw_eventfd = true;
						self.async_notification_pending
							.store(false, Ordering::Release);
						continue;
					}

					let slot = completion.user_data().to_usize().unwrap();
					let Some(slot_data) = self.slots.get_mut(slot) else {
						return Err(Error::other(format!(
							"received an io_uring completion for invalid slot {slot}",
						)));
					};

					if completion.result() < 0 {
						let error = -completion.result();
						if let UringSlotState::Commit { unique } = slot_data.state {
							self.server
								.rollback_response_resources(self.fd.as_ref(), unique);
						}
						if error == libc::EAGAIN
							&& matches!(slot_data.state, UringSlotState::Register)
						{
							register_retries.push(slot);
							continue;
						}
						if error == libc::ENOENT
							&& matches!(slot_data.state, UringSlotState::Commit { .. })
						{
							stale_commits.push(slot);
							continue;
						}
						if matches!(error, libc::ECONNABORTED | libc::ENODEV | libc::ENOTCONN) {
							return Ok(());
						}
						return Err(Error::other(format!(
							"io_uring slot {slot} on queue {} failed in state {:?}: {}",
							slot_data.qid,
							slot_data.state,
							Error::from_raw_os_error(error),
						)));
					}

					if !matches!(
						slot_data.state,
						UringSlotState::Commit { .. } | UringSlotState::Register
					) {
						return Err(Error::other(
							"received an io_uring request completion for a busy slot",
						));
					}

					if let UringSlotState::Commit { unique } = slot_data.state {
						self.server.commit_response_resources(unique);
					}
					let request = Server::<P>::deserialize_uring_request(slot_data)?;
					slot_data.state = UringSlotState::Request {
						opcode: request.header.opcode,
						unique: request.header.unique,
					};
					batch_requests.push(PendingRequest { request, slot });
				}
			}
			for slot in register_retries.drain(..) {
				Server::<P>::submit_register(
					&mut self.io_uring,
					types::Fixed(THREAD_FIXED_FUSE_FD),
					slot,
					self.slots.get_mut(slot).unwrap(),
				)?;
			}
			for slot in stale_commits.drain(..) {
				Server::<P>::replace_stale_uring_slot(
					&mut self.slots,
					&mut self.retired_slots,
					self.payload_size,
					slot,
				)?;
				Server::<P>::submit_register(
					&mut self.io_uring,
					types::Fixed(THREAD_FIXED_FUSE_FD),
					slot,
					self.slots.get_mut(slot).unwrap(),
				)?;
			}
			self.server.handle_request_sync_batch(
				self.fd.as_ref(),
				&batch_requests,
				&mut batch_results,
			)?;
			commits.clear();
			deferred.clear();
			for (pending_request, result) in
				std::iter::zip(batch_requests.drain(..), batch_results.drain(..))
			{
				let slot = pending_request.slot;
				let unique = pending_request.request.header.unique;
				let opcode = pending_request.request.header.opcode;
				match result {
					Dispatch::Deferred => deferred.push(pending_request),
					Dispatch::Ready(result) => {
						if let Err(error) = &result {
							let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
							if !Server::<P>::is_expected_error(opcode, error_code) {
								tracing::error!(?error, ?opcode, worker_id = %self.worker_id, "unexpected error");
							}
						}
						commits.push((slot, unique, result));
					},
				}
			}
			for pending_request in deferred.drain(..) {
				let PendingRequest { request, slot } = pending_request;
				let unique = request.header.unique;
				let opcode = request.header.opcode;
				let token = self.server.register_async_request(unique)?;
				let slot_data = self.slots.get_mut(slot).unwrap();
				if !matches!(
					slot_data.state,
					UringSlotState::Request {
						opcode: expected_opcode,
						unique: expected_unique,
					} if expected_opcode == opcode && expected_unique == unique
				) {
					self.server.finish_async_request(unique);
					return Err(Error::other("the io_uring slot request state changed"));
				}
				slot_data.state = UringSlotState::Async { opcode, unique };
				self.server.spawn_async_request(AsyncRequestContext {
					async_notification_pending: self.async_notification_pending.clone(),
					eventfd: self.eventfd.clone(),
					fd: self.fd.clone(),
					request,
					runtime: self.runtime.clone(),
					sender: self.sender.clone(),
					slot,
					token,
					worker_event_sender: self.event_sender.clone(),
					worker_id: self.worker_id,
				});
			}
			for (slot, unique, result) in commits.drain(..) {
				self.server.submit_commit_and_fetch(
					&mut self.io_uring,
					self.fd.as_ref(),
					slot,
					self.slots.get_mut(slot).unwrap(),
					unique,
					result,
				)?;
			}

			if saw_eventfd || !self.receiver.is_empty() {
				self.server.drain_async_responses(
					self.worker_id,
					self.fd.as_ref(),
					&mut self.io_uring,
					&mut self.slots,
					&self.receiver,
				)?;
			}
		}
	}
}

impl UringSlot {
	#[must_use]
	pub(in crate::fuse) fn new(qid: u16, payload_size: usize) -> Self {
		let mut header = Box::new(sys::fuse_uring_req_header::new_zeroed());
		let mut payload = vec![0u8; payload_size];
		let iovecs = [
			libc::iovec {
				iov_base: std::ptr::from_mut(&mut *header).cast(),
				iov_len: size_of::<sys::fuse_uring_req_header>(),
			},
			libc::iovec {
				iov_base: payload.as_mut_ptr().cast(),
				iov_len: payload.len(),
			},
		];
		Self {
			header,
			iovecs,
			payload,
			qid,
			state: UringSlotState::Register,
		}
	}
}
