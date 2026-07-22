use super::{session::notify_async_response, *};

pub(super) mod worker;

const FUSE_MIN_READ_BUFFER: usize = 8192;
const IO_URING_MAX_READER_COUNT: usize = 8;
const IO_URING_MAX_SLOTS_PER_QUEUE: usize = 4;
const IO_URING_PAYLOAD_MEMORY_BUDGET: usize = 256 * 1024 * 1024;
const IO_URING_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
pub(super) struct RingConfig {
	pub(super) limits: RequestLimits,
	pub(super) queue_count: usize,
	pub(super) slots_per_queue: usize,
	pub(super) worker_count: usize,
}

pub(super) struct RingStartupContext<'a> {
	pub(super) connection_id: u64,
	pub(super) event_receiver: &'a mut tokio::sync::mpsc::UnboundedReceiver<WorkerEvent>,
	pub(super) event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	pub(super) fd: Arc<OwnedFd>,
	pub(super) path: &'a Path,
	pub(super) ring_config: RingConfig,
	pub(super) runtime: tokio::runtime::Handle,
	pub(super) sqpoll_wq_fd: RawFd,
}

pub(super) struct RingStartupFailure {
	pub(super) disconnected: bool,
	pub(super) error: Error,
}

struct RingWorkerConfig {
	event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	payload_size: usize,
	queue_ids: Vec<u16>,
	slots_per_queue: usize,
	sqpoll_wq_fd: RawFd,
	worker_id: usize,
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn ring_config(page_size: usize) -> Result<RingConfig> {
		// Derive the per-queue payload budget.
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

		// Derive the worker count.
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
		// Parse the possible CPU ranges.
		let mut cpus = BTreeSet::new();
		for range in possible.trim().split(',') {
			if range.is_empty() {
				return Err(Error::other("the possible CPU set contains an empty range"));
			}
			let (start, end) = match range.split_once('-') {
				None => (range, range),
				Some((start, end)) => (start, end),
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

		// Validate that queue IDs form a dense range from zero.
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
		// Derive the page payload size.
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

		// Derive the request buffer dimensions.
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
			// Prepare the control descriptor.
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

			// Start the queue workers.
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
						if let Err(error) = server.thread_loop(&thread_fd, &runtime, worker_config)
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

			// Wait for the workers to become ready.
			for _ in 0..ring_config.worker_count {
				match event_receiver.recv().await {
					None => return Err(Error::other("an io_uring worker failed during startup")),
					Some(WorkerEvent::Failed { error, worker }) => {
						return Err(Error::other(format!(
							"{worker} failed during startup: {error}",
						)));
					},
					Some(WorkerEvent::Ready) => {},
				}
			}

			// Confirm that the kernel can dispatch through io_uring.
			Self::probe_io_uring(path).await?;
			while let Ok(event) = event_receiver.try_recv() {
				if let WorkerEvent::Failed { error, worker } = event {
					return Err(Error::other(format!(
						"{worker} failed during startup: {error}",
					)));
				}
			}

			// Start the control request reader.
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

		// Clean up a partial startup.
		if let Err(error) = startup {
			self.cancel_async_requests();
			let disconnected = Self::disconnect_transport(path, connection_id).await;
			Self::join_transport_threads(&mut thread_handles, disconnected);
			if disconnected {
				self.rollback_all_response_resources(fd.as_ref());
			}
			while event_receiver.try_recv().is_ok() {}

			return Err(RingStartupFailure {
				disconnected,
				error,
			});
		}

		Ok(thread_handles)
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
}
