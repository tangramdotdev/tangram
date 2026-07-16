use super::*;

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn ring_config(page_size: usize) -> Result<RingConfig> {
		if page_size == 0 {
			return Err(Error::other("the system page size is zero"));
		}
		let queue_count = Self::possible_cpu_count()?;
		let max_payload_size = IO_URING_PAYLOAD_MEMORY_BUDGET
			.checked_div(queue_count)
			.ok_or_else(|| Error::other("the io_uring queue count is zero"))?;
		let max_write = DEFAULT_MAX_WRITE.min(max_payload_size) / page_size * page_size;
		let limits = Self::request_limits(page_size, max_write)?;
		if limits.payload_size > max_payload_size {
			return Err(Error::other(format!(
				"the io_uring payload memory budget cannot cover {queue_count} kernel queues",
			)));
		}
		let payload_memory_per_slot = queue_count
			.checked_mul(limits.payload_size)
			.ok_or_else(|| Error::other("the io_uring payload memory size overflowed"))?;
		let slots_per_queue = IO_URING_PAYLOAD_MEMORY_BUDGET
			.checked_div(payload_memory_per_slot)
			.unwrap_or(0)
			.clamp(1, IO_URING_MAX_SLOTS_PER_QUEUE);
		let available_parallelism =
			std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
		let worker_count = available_parallelism
			.min(queue_count)
			.clamp(1, IO_URING_MAX_READER_COUNT);

		Ok(RingConfig {
			limits,
			queue_count,
			slots_per_queue,
			worker_count,
		})
	}

	fn possible_cpu_count() -> Result<usize> {
		let possible =
			std::fs::read_to_string("/sys/devices/system/cpu/possible").map_err(|error| {
				Error::other(format!("failed to read the possible CPU set: {error}"))
			})?;
		Self::parse_possible_cpu_count(&possible)
	}

	pub(super) fn parse_possible_cpu_count(possible: &str) -> Result<usize> {
		let mut cpus = BTreeSet::new();
		for range in possible.trim().split(',') {
			if range.is_empty() {
				return Err(Error::other("the possible CPU set contains an empty range"));
			}
			let (start, end) = match range.split_once('-') {
				Some((start, end)) => (start, end),
				None => (range, range),
			};
			let start = start.parse::<usize>().map_err(|error| {
				Error::other(format!("failed to parse a possible CPU range: {error}"))
			})?;
			let end = end.parse::<usize>().map_err(|error| {
				Error::other(format!("failed to parse a possible CPU range: {error}"))
			})?;
			if start > end {
				return Err(Error::other("a possible CPU range is reversed"));
			}
			if end > u16::MAX.to_usize().unwrap() {
				return Err(Error::other(
					"the possible CPU set exceeds the FUSE queue ID range",
				));
			}
			cpus.extend(start..=end);
		}
		let Some(first) = cpus.first() else {
			return Err(Error::other("the possible CPU set is empty"));
		};
		let last = cpus.last().unwrap();
		if *first != 0 || last + 1 != cpus.len() {
			return Err(Error::other(
				"the possible CPU set is not contiguous from CPU zero",
			));
		}

		Ok(cpus.len())
	}

	pub(super) fn request_limits(page_size: usize, max_write: usize) -> Result<RequestLimits> {
		if page_size == 0 || max_write == 0 {
			return Err(Error::other("the FUSE request dimensions must be non-zero"));
		}
		let max_pages = max_write.div_ceil(page_size).max(1);
		let max_pages = max_pages
			.to_u16()
			.ok_or_else(|| Error::other("the FUSE max page count is out of range"))?;
		let page_payload_size = max_pages
			.to_usize()
			.unwrap()
			.checked_mul(page_size)
			.ok_or_else(|| Error::other("the FUSE page payload size overflowed"))?;
		let payload_size = FUSE_MIN_READ_BUFFER.max(max_write).max(page_payload_size);
		let request_buffer_size = payload_size
			.checked_add(page_size)
			.ok_or_else(|| Error::other("the FUSE request buffer size overflowed"))?;
		let max_write = max_write
			.to_u32()
			.ok_or_else(|| Error::other("the FUSE max write size is out of range"))?;

		Ok(RequestLimits {
			max_pages,
			max_write,
			payload_size,
			request_buffer_size,
		})
	}

	pub(super) async fn probe_io_uring(path: &Path) -> Result<()> {
		let path = path.to_owned();
		tokio::task::spawn_blocking(move || {
			let mut entries = std::fs::read_dir(path)?;
			entries.next().transpose()?;
			Ok(())
		})
		.await
		.map_err(|error| Error::other(format!("the io_uring readiness probe failed: {error}")))?
	}

	pub(super) async fn start_ring_transport(
		&self,
		context: RingStartupContext<'_>,
	) -> std::result::Result<Vec<std::thread::JoinHandle<()>>, RingStartupFailure> {
		let RingStartupContext {
			connection_id,
			event_receiver,
			event_sender,
			fd,
			path,
			ring_config,
			runtime,
			sqpoll_wq_fd,
		} = context;
		let mut thread_handles = Vec::new();
		let startup = async {
			let control_fd = Self::clone_thread_fd(fd.as_ref())
				.map(Arc::new)
				.map_err(|error| {
					Error::other(format!("failed to clone the io_uring control fd: {error}"))
				})?;
			tracing::info!(
				payload_memory = ring_config.queue_count
					* ring_config.slots_per_queue
					* ring_config.limits.payload_size,
				payload_size = ring_config.limits.payload_size,
				queue_count = ring_config.queue_count,
				slots_per_queue = ring_config.slots_per_queue,
				worker_count = ring_config.worker_count,
				"starting the FUSE io_uring transport",
			);

			for worker_id in 0..ring_config.worker_count {
				let queue_ids = (worker_id..ring_config.queue_count)
					.step_by(ring_config.worker_count)
					.map(|queue_id| queue_id.to_u16().unwrap())
					.collect::<Vec<_>>();
				let server = self.clone();
				let runtime = runtime.clone();
				let worker_event_sender = event_sender.clone();
				let thread_fd = fd.clone();
				let worker_config = RingWorkerConfig {
					event_sender: worker_event_sender,
					payload_size: ring_config.limits.payload_size,
					queue_ids,
					slots_per_queue: ring_config.slots_per_queue,
					sqpoll_wq_fd,
					worker_id,
				};
				let thread = std::thread::Builder::new()
					.name(format!("tangram-fuse-io-uring-{worker_id}"))
					.spawn(move || {
						let worker_event_sender = worker_config.event_sender.clone();
						if let Err(error) =
							server.thread_loop(thread_fd.as_ref(), &runtime, worker_config)
						{
							if error.raw_os_error() == Some(libc::ENOTCONN) {
								tracing::debug!(%error, %worker_id, "io_uring worker exited during shutdown");
							} else {
								tracing::error!(%error, %worker_id, "io_uring worker failed");
								worker_event_sender
									.send(WorkerEvent::Failed {
										error,
										worker: format!("io_uring worker {worker_id}"),
									})
									.ok();
							}
						}
					})
					.map_err(|error| {
						Error::other(format!(
							"failed to spawn the io_uring worker {worker_id}: {error}",
						))
					})?;
				thread_handles.push(thread);
			}

			for _ in 0..ring_config.worker_count {
				match event_receiver.recv().await {
					Some(WorkerEvent::Ready) => {},
					Some(WorkerEvent::Failed { error, worker }) => {
						return Err(Error::other(format!(
							"{worker} failed during startup: {error}",
						)));
					},
					None => return Err(Error::other("an io_uring worker failed during startup")),
				}
			}

			Self::probe_io_uring(path).await?;
			while let Ok(event) = event_receiver.try_recv() {
				if let WorkerEvent::Failed { error, worker } = event {
					return Err(Error::other(format!(
						"{worker} failed during startup: {error}",
					)));
				}
			}

			let server = self.clone();
			let worker_event_sender = event_sender.clone();
			let thread = std::thread::Builder::new()
				.name("tangram-fuse-io-uring-control".to_owned())
				.spawn(move || {
					if let Err(error) = server
						.thread_loop_control(&control_fd, ring_config.limits.request_buffer_size)
					{
						if matches!(error.raw_os_error(), Some(libc::ENODEV | libc::ENOTCONN)) {
							tracing::debug!(%error, "io_uring control reader exited during shutdown");
						} else {
							tracing::error!(%error, "io_uring control reader failed");
							worker_event_sender
								.send(WorkerEvent::Failed {
									error,
									worker: "io_uring control reader".to_owned(),
								})
								.ok();
						}
					}
				})
				.map_err(|error| {
					Error::other(format!(
						"failed to spawn the io_uring control reader: {error}"
					))
				})?;
			thread_handles.push(thread);

			Ok(())
		};
		let startup = tokio::time::timeout(IO_URING_STARTUP_TIMEOUT, startup)
			.await
			.map_err(|_| Error::other("timed out waiting for the io_uring transport"))
			.and_then(std::convert::identity);
		if let Err(error) = startup {
			self.cancel_async_requests();
			let disconnected = Self::disconnect_transport(path, connection_id).await;
			Self::join_transport_threads(&mut thread_handles, disconnected);
			if disconnected {
				self.rollback_all_response_resources(fd.as_raw_fd());
			}
			while event_receiver.try_recv().is_ok() {}

			return Err(RingStartupFailure {
				disconnected,
				error,
			});
		}

		Ok(thread_handles)
	}

	pub(super) fn thread_loop(
		&self,
		fd: &OwnedFd,
		runtime: &tokio::runtime::Handle,
		config: RingWorkerConfig,
	) -> Result<()> {
		let RingWorkerConfig {
			event_sender,
			payload_size,
			queue_ids,
			slots_per_queue,
			sqpoll_wq_fd,
			worker_id,
		} = config;
		let eventfd = rustix::event::eventfd(0, EventfdFlags::CLOEXEC).map_err(|error| {
			Error::other(format!("failed to create the thread eventfd: {error}"))
		})?;
		let eventfd = Arc::new(eventfd);
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
		let mut builder = IoUring::<io_uring::squeue::Entry128>::builder();
		builder.setup_attach_wq(sqpoll_wq_fd).setup_no_sqarray();
		let mut io_uring = builder
			.build(ring_entries)
			.map_err(|error| Error::other(format!("failed to build the thread ring: {error}")))?;
		io_uring
			.submitter()
			.register_files(&[fd.as_raw_fd(), eventfd.as_raw_fd()])
			.map_err(|error| {
				Error::other(format!("failed to register the thread files: {error}"))
			})?;
		let mut slots = queue_ids
			.iter()
			.flat_map(|&qid| {
				(0..slots_per_queue).map(move |_| Box::new(UringSlot::new(qid, payload_size)))
			})
			.collect::<Vec<_>>();
		let mut retired_slots = VecDeque::new();
		let async_notification_pending = Arc::new(AtomicBool::new(false));
		let mut eventfd_inflight = false;
		let mut eventfd_buffer = [0u8; size_of::<u64>()];
		let mut batch_requests = Vec::<PendingRequest>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut batch_results =
			Vec::<Result<Option<Response>>>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut commits =
			Vec::<(usize, u64, Result<Option<Response>>)>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut deferred = Vec::<PendingRequest>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut register_retries = Vec::<usize>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let mut stale_commits = Vec::<usize>::with_capacity(THREAD_CQE_BATCH_SIZE);
		let registered_buffers = [libc::iovec {
			iov_base: eventfd_buffer.as_mut_ptr().cast(),
			iov_len: eventfd_buffer.len(),
		}];
		unsafe {
			io_uring
				.submitter()
				.register_buffers(&registered_buffers)
				.map_err(|error| {
					Error::other(format!("failed to register the thread buffers: {error}"))
				})?;
		}

		{
			let mut submission = io_uring.submission();
			for (slot, slot_data) in slots.iter().enumerate() {
				let entry = Self::build_fuse_uring_cmd_entry(
					types::Fixed(THREAD_FIXED_FUSE_FD),
					sys::fuse_uring_cmd_FUSE_IO_URING_CMD_REGISTER,
					0,
					slot_data.qid,
					slot_data.iovecs.as_ptr(),
					slot.to_u64().unwrap(),
					0,
				);
				if unsafe { submission.push(&entry) }.is_err() {
					return Err(Error::other("failed to submit uring register command"));
				}
			}
		}
		io_uring.submit().map_err(|error| {
			Error::other(format!("failed to submit register commands: {error}"))
		})?;
		event_sender.send(WorkerEvent::Ready).ok();

		loop {
			{
				let mut submission = io_uring.submission();
				if !eventfd_inflight {
					let entry: io_uring::squeue::Entry128 = opcode::ReadFixed::new(
						types::Fixed(THREAD_FIXED_EVENTFD_FD),
						eventfd_buffer.as_mut_ptr(),
						eventfd_buffer.len().to_u32().unwrap(),
						EVENTFD_BUFFER_INDEX,
					)
					.offset(u64::MAX)
					.build()
					.user_data(EVENTFD_USER_DATA)
					.into();
					if unsafe { submission.push(&entry) }.is_ok() {
						eventfd_inflight = true;
					}
				}
			}

			if let Err(error) = io_uring.submit_and_wait(1) {
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
				let mut completion = io_uring.completion();
				for completion in completion.by_ref().take(THREAD_CQE_BATCH_SIZE) {
					if completion.user_data() == EVENTFD_USER_DATA {
						eventfd_inflight = false;
						saw_eventfd = true;
						async_notification_pending.store(false, Ordering::Release);
						continue;
					}

					let slot = completion.user_data().to_usize().unwrap();
					let Some(slot_data) = slots.get_mut(slot) else {
						return Err(Error::other(format!(
							"received an io_uring completion for invalid slot {slot}",
						)));
					};

					if completion.result() < 0 {
						let error = -completion.result();
						if let UringSlotState::Commit { unique } = slot_data.state {
							self.rollback_response_resources(fd.as_raw_fd(), unique);
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
						self.commit_response_resources(unique);
					}
					let request = Self::deserialize_uring_request(slot_data)?;
					slot_data.state = UringSlotState::Request {
						opcode: request.header.opcode,
						unique: request.header.unique,
					};
					batch_requests.push(PendingRequest { slot, request });
				}
			}
			for slot in register_retries.drain(..) {
				Self::submit_register(
					&mut io_uring,
					types::Fixed(THREAD_FIXED_FUSE_FD),
					slot,
					slots.get_mut(slot).unwrap(),
				)?;
			}
			for slot in stale_commits.drain(..) {
				Self::replace_stale_uring_slot(&mut slots, &mut retired_slots, payload_size, slot)?;
				Self::submit_register(
					&mut io_uring,
					types::Fixed(THREAD_FIXED_FUSE_FD),
					slot,
					slots.get_mut(slot).unwrap(),
				)?;
			}
			self.handle_request_sync_batch(fd.as_raw_fd(), &batch_requests, &mut batch_results)?;
			commits.clear();
			deferred.clear();
			for (pending_request, result) in
				std::iter::zip(batch_requests.drain(..), batch_results.drain(..))
			{
				let slot = pending_request.slot;
				let unique = pending_request.request.header.unique;
				let opcode = pending_request.request.header.opcode;
				match result {
					Ok(Some(response)) => {
						commits.push((slot, unique, Ok(Some(response))));
					},
					Ok(None) => {
						deferred.push(pending_request);
					},
					Err(error)
						if error.raw_os_error() == Some(libc::ENOSYS)
							&& opcode != sys::fuse_opcode_FUSE_OPENDIR =>
					{
						deferred.push(pending_request);
					},
					Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
						commits.push((slot, unique, Err(error)));
					},
					Err(error) => {
						let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
						if !Self::is_expected_error(opcode, error_code) {
							tracing::error!(?error, ?opcode, %worker_id, "unexpected error");
						}
						commits.push((slot, unique, Err(error)));
					},
				}
			}
			for pending_request in deferred.drain(..) {
				let PendingRequest { slot, request } = pending_request;
				let unique = request.header.unique;
				let opcode = request.header.opcode;
				let token = self.register_async_request(unique)?;
				let slot_data = slots.get_mut(slot).unwrap();
				if !matches!(
					slot_data.state,
					UringSlotState::Request {
						opcode: expected_opcode,
						unique: expected_unique,
					} if expected_opcode == opcode && expected_unique == unique
				) {
					self.finish_async_request(unique);
					return Err(Error::other("the io_uring slot request state changed"));
				}
				slot_data.state = UringSlotState::Async { opcode, unique };
				self.spawn_async_request(AsyncRequestContext {
					async_notification_pending: async_notification_pending.clone(),
					eventfd: eventfd.clone(),
					request,
					runtime: runtime.clone(),
					sender: sender.clone(),
					slot,
					token,
					worker_event_sender: event_sender.clone(),
					worker_id,
				});
			}
			for (slot, unique, result) in commits.drain(..) {
				self.submit_commit_and_fetch(
					&mut io_uring,
					fd.as_raw_fd(),
					slot,
					slots.get_mut(slot).unwrap(),
					unique,
					result,
				)?;
			}

			if saw_eventfd || !receiver.is_empty() {
				self.drain_async_responses(
					worker_id,
					fd.as_raw_fd(),
					&mut io_uring,
					&mut slots,
					&receiver,
				)?;
			}
		}
	}

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
			flags: command_flags,
			commit_id,
			qid,
			padding: [0; 6],
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
		if unsafe { submission.push(&entry) }.is_err() {
			return Err(Error::other(
				"failed to submit an io_uring register command",
			));
		}

		Ok(())
	}

	pub(super) fn replace_stale_uring_slot(
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
		response_fd: RawFd,
		slot: usize,
		slot_data: &mut UringSlot,
		unique: u64,
		result: Result<Option<Response>>,
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
		result: Result<Option<Response>>,
	) -> Result<()> {
		let in_out = unsafe {
			std::slice::from_raw_parts_mut(
				slot_data.header.in_out.as_mut_ptr().cast::<u8>(),
				slot_data.header.in_out.len(),
			)
		};
		in_out.fill(0);
		let (error, payload_len, needs_header) = match result {
			Ok(Some(response)) => {
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
			Ok(None) => (0, 0, true),
			Err(error) => (error.raw_os_error().unwrap_or(libc::ENOSYS), 0, true),
		};
		if needs_header {
			let len = size_of::<fuse_out_header>() + payload_len;
			let out = fuse_out_header {
				unique,
				len: len.to_u32().unwrap(),
				error: -error,
			};
			in_out[..size_of::<fuse_out_header>()].copy_from_slice(out.as_bytes());
		}
		slot_data.header.ring_ent_in_out = sys::fuse_uring_ent_in_out {
			flags: 0,
			commit_id: unique,
			payload_sz: payload_len.to_u32().unwrap(),
			padding: 0,
			reserved: 0,
		};
		Ok(())
	}

	fn spawn_async_request(&self, context: AsyncRequestContext) {
		let AsyncRequestContext {
			async_notification_pending,
			eventfd,
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
				.handle_cancellable_request(request, token)
				.await
				.inspect_err(|error| {
					let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
					if !Self::is_expected_error(opcode, error_code) {
						tracing::error!(?error, ?opcode, "unexpected error");
					}
				});
			let response = AsyncResponse {
				opcode,
				result,
				slot,
				unique,
			};
			notify_async_response(
				&async_notification_pending,
				eventfd.as_raw_fd(),
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
		response_fd: RawFd,
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

impl UringSlot {
	pub(super) fn new(qid: u16, payload_size: usize) -> Self {
		let mut header = Box::new(unsafe { std::mem::zeroed::<sys::fuse_uring_req_header>() });
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
