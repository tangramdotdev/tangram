use super::{session::notify_async_response, *};

struct TestProvider;
struct PassthroughProvider {
	closes: Arc<std::sync::atomic::AtomicUsize>,
}
struct SlowProvider {
	release: Arc<tokio::sync::Notify>,
	started: Arc<tokio::sync::Notify>,
}

impl Provider for TestProvider {
	fn handle_batch(
		&self,
		_requests: Vec<ProviderRequest>,
	) -> impl futures::Future<Output = Vec<Result<ProviderResponse>>> + Send {
		std::future::ready(Vec::new())
	}

	fn handle_batch_sync(&self, _requests: Vec<ProviderRequest>) -> Vec<Result<ProviderResponse>> {
		Vec::new()
	}
}

impl Provider for PassthroughProvider {
	fn handle_batch(
		&self,
		requests: Vec<ProviderRequest>,
	) -> impl futures::Future<Output = Vec<Result<ProviderResponse>>> + Send {
		let responses = requests
			.into_iter()
			.map(|request| match request {
				ProviderRequest::Close { .. } => {
					self.closes.fetch_add(1, Ordering::Relaxed);
					Ok(ProviderResponse::Unit)
				},
				ProviderRequest::Open { .. } => Ok(ProviderResponse::Open {
					backing_fd: None,
					handle: 7,
				}),
				_ => Err(Error::from_raw_os_error(libc::ENOSYS)),
			})
			.collect();

		std::future::ready(responses)
	}

	fn handle_batch_sync(&self, requests: Vec<ProviderRequest>) -> Vec<Result<ProviderResponse>> {
		requests
			.into_iter()
			.map(|request| match request {
				ProviderRequest::Close { .. } => {
					self.closes.fetch_add(1, Ordering::Relaxed);
					Ok(ProviderResponse::Unit)
				},
				_ => Err(Error::from_raw_os_error(libc::ENOSYS)),
			})
			.collect()
	}
}

impl Provider for SlowProvider {
	fn handle_batch(
		&self,
		requests: Vec<ProviderRequest>,
	) -> impl futures::Future<Output = Vec<Result<ProviderResponse>>> + Send {
		let release = self.release.clone();
		let started = self.started.clone();
		async move {
			let mut responses = Vec::with_capacity(requests.len());
			for request in requests {
				let response = match request {
					ProviderRequest::GetAttr { .. } => Ok(ProviderResponse::GetAttr {
						attrs: crate::Attrs::new(AttrsInner::File {
							executable: false,
							size: 4,
						}),
					}),
					ProviderRequest::Read { .. } => {
						started.notify_one();
						release.notified().await;
						Ok(ProviderResponse::Read {
							bytes: Bytes::from_static(b"slow"),
						})
					},
					_ => Err(Error::from_raw_os_error(libc::ENOSYS)),
				};
				responses.push(response);
			}
			responses
		}
	}

	fn handle_batch_sync(&self, requests: Vec<ProviderRequest>) -> Vec<Result<ProviderResponse>> {
		requests
			.into_iter()
			.map(|_| Err(Error::from_raw_os_error(libc::ENOSYS)))
			.collect()
	}
}

fn server_with_provider<P>(
	provider: P,
	passthrough_enabled: bool,
	passthrough_required: bool,
) -> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	Server(Arc::new(State {
		active_requests: Mutex::new(HashMap::new()),
		no_opendir_support: false,
		pending_response_resources: Mutex::new(HashMap::new()),
		passthrough_backing_ids: Mutex::new(HashMap::new()),
		passthrough_enabled,
		passthrough_permission_warning_emitted: AtomicBool::new(false),
		passthrough_required,
		provider,
		task: Mutex::new(None),
	}))
}

fn server() -> Server<TestProvider> {
	server_with_provider(TestProvider, false, false)
}

fn test_fd() -> OwnedFd {
	std::fs::File::open("/dev/null").unwrap().into()
}

fn open_request(unique: u64) -> Request {
	Request {
		data: RequestData::Open(fuse_open_in {
			flags: 0,
			open_flags: 0,
		}),
		header: request_header(sys::fuse_opcode_FUSE_OPEN, unique),
	}
}

fn request_header(opcode: sys::fuse_opcode, unique: u64) -> fuse_in_header {
	fuse_in_header {
		gid: 0,
		len: 0,
		nodeid: 2,
		opcode,
		padding: 0,
		pid: 0,
		total_extlen: 0,
		uid: 0,
		unique,
	}
}

async fn read_response(fd: Arc<OwnedFd>) -> fuse_out_header {
	let response = tokio::task::spawn_blocking(move || {
		let mut bytes = Vec::new();
		loop {
			let mut buffer = [0u8; 4096];
			let size = rustix::io::read(fd.as_ref(), &mut buffer).unwrap();
			assert_ne!(size, 0);
			bytes.extend_from_slice(&buffer[..size]);
			if bytes.len() < size_of::<fuse_out_header>() {
				continue;
			}
			let (header, _) = fuse_out_header::read_from_prefix(&bytes).unwrap();
			if bytes.len() >= header.len.to_usize().unwrap() {
				return header;
			}
		}
	});
	tokio::time::timeout(Duration::from_secs(2), response)
		.await
		.unwrap()
		.unwrap()
}

#[test]
fn parses_possible_cpu_sets() {
	assert_eq!(
		Server::<TestProvider>::parse_possible_cpu_count("0\n").unwrap(),
		1,
	);
	assert_eq!(
		Server::<TestProvider>::parse_possible_cpu_count("0-3,4-15\n").unwrap(),
		16,
	);
	assert!(Server::<TestProvider>::parse_possible_cpu_count("0-3,8-11").is_err());
}

#[test]
fn readlink_responses_validate_targets_without_copying() {
	let target = Bytes::from_static(b"target");
	let pointer = target.as_ptr();
	let response = Server::<TestProvider>::read_link_response(target).unwrap();
	let Response::ReadLink(target) = response else {
		panic!("expected a readlink response");
	};
	assert_eq!(target.as_ptr(), pointer);

	let error =
		Server::<TestProvider>::read_link_response(Bytes::from_static(b"bad\0target")).unwrap_err();
	assert_eq!(error.raw_os_error(), Some(libc::EIO));
}

#[test]
fn stale_uring_slots_are_replaced_with_distinct_buffers() {
	let mut slots = vec![Box::new(UringSlot::new(7, 4096))];
	slots[0].state = UringSlotState::Commit { unique: 42 };
	let header = std::ptr::from_ref(slots[0].header.as_ref());
	let payload = slots[0].payload.as_ptr();
	let mut retired_slots = VecDeque::new();

	Server::<TestProvider>::replace_stale_uring_slot(&mut slots, &mut retired_slots, 4096, 0)
		.unwrap();

	assert_eq!(retired_slots.len(), 1);
	assert_eq!(slots[0].qid, 7);
	assert!(matches!(slots[0].state, UringSlotState::Register));
	assert_ne!(std::ptr::from_ref(slots[0].header.as_ref()), header);
	assert_ne!(slots[0].payload.as_ptr(), payload);
}

#[test]
fn parses_fuse_connection_ids_without_accessing_the_mount() {
	let mountinfo = concat!(
		"20 1 8:1 / / rw - ext4 /dev/root rw\n",
		"21 20 0:42 / /tmp/mount\\040point rw - fuse tangram rw\n",
	);
	let connection_id =
		Server::<TestProvider>::parse_connection_id(mountinfo, Path::new("/tmp/mount point"))
			.unwrap();

	assert_eq!(connection_id, libc::makedev(0, 42));
}

#[test]
fn derives_request_limits_from_page_size() {
	let limits = Server::<TestProvider>::request_limits(64 * 1024, DEFAULT_MAX_WRITE).unwrap();
	assert_eq!(limits.max_pages, 16);
	assert_eq!(limits.max_write, DEFAULT_MAX_WRITE.to_u32().unwrap());
	assert_eq!(limits.payload_size, DEFAULT_MAX_WRITE);
	assert_eq!(limits.request_buffer_size, DEFAULT_MAX_WRITE + 64 * 1024);
}

#[test]
fn malformed_names_fail_individual_batch_requests() {
	let server = server();
	let lookup_header = fuse_in_header {
		gid: 0,
		len: 0,
		nodeid: 0,
		opcode: sys::fuse_opcode_FUSE_LOOKUP,
		padding: 0,
		pid: 0,
		total_extlen: 0,
		uid: 0,
		unique: 1,
	};
	let invalid_name = CString::new(vec![0xff]).unwrap();
	let getxattr_header = fuse_in_header {
		opcode: sys::fuse_opcode_FUSE_GETXATTR,
		unique: 2,
		..lookup_header
	};
	let statfs_header = fuse_in_header {
		opcode: sys::fuse_opcode_FUSE_STATFS,
		unique: 3,
		..lookup_header
	};
	let requests = [
		PendingRequest {
			request: Request {
				data: RequestData::Lookup(invalid_name.clone()),
				header: lookup_header,
			},
			slot: 0,
		},
		PendingRequest {
			request: Request {
				data: RequestData::GetXattr(
					sys::fuse_getxattr_in {
						padding: 0,
						size: 0,
					},
					invalid_name,
				),
				header: getxattr_header,
			},
			slot: 1,
		},
		PendingRequest {
			request: Request {
				data: RequestData::Statfs,
				header: statfs_header,
			},
			slot: 2,
		},
	];
	let mut results = Vec::new();
	let fd = test_fd();
	server
		.handle_request_sync_batch(&fd, &requests, &mut results)
		.unwrap();

	assert_eq!(results.len(), 3);
	let Dispatch::Ready(result) = &results[0] else {
		panic!("expected a ready response");
	};
	assert_eq!(
		result.as_ref().unwrap_err().raw_os_error(),
		Some(libc::ENOENT)
	);
	let Dispatch::Ready(result) = &results[1] else {
		panic!("expected a ready response");
	};
	assert_eq!(
		result.as_ref().unwrap_err().raw_os_error(),
		Some(libc::ENODATA)
	);
	assert!(matches!(
		results[2],
		Dispatch::Ready(Ok(Response::Statfs(_)))
	));
}

#[test]
fn required_passthrough_failure_closes_provider_handle() {
	let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let server = server_with_provider(
		PassthroughProvider {
			closes: closes.clone(),
		},
		true,
		true,
	);
	let request = open_request(1);
	let response = ProviderResponse::Open {
		backing_fd: None,
		handle: 7,
	};
	let fd = test_fd();
	let error = server
		.map_provider_response_sync(&fd, &request, response)
		.unwrap_err();

	assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP));
	assert_eq!(closes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn required_passthrough_failure_closes_provider_handle_async() {
	let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let server = server_with_provider(
		PassthroughProvider {
			closes: closes.clone(),
		},
		true,
		true,
	);
	let fd = test_fd();
	let error = server
		.handle_request(&fd, open_request(1))
		.await
		.unwrap_err();

	assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP));
	assert_eq!(closes.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_open_response_closes_provider_handle() {
	let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let server = server_with_provider(
		PassthroughProvider {
			closes: closes.clone(),
		},
		false,
		false,
	);
	let result = Ok(Response::Open(fuse_open_out {
		backing_id: -1,
		fh: 11,
		open_flags: 0,
	}));
	let fd = test_fd();
	server.register_response_resources(&fd, 1, &result).unwrap();
	server.rollback_response_resources(&fd, 1);

	assert_eq!(closes.load(Ordering::Relaxed), 1);
}

#[test]
fn statx_reports_supported_metadata() {
	let attrs = crate::Attrs {
		atime: crate::Timestamp { nanos: 2, secs: 1 },
		ctime: crate::Timestamp { nanos: 6, secs: 5 },
		gid: 8,
		inner: AttrsInner::Symlink { size: 513 },
		mtime: crate::Timestamp { nanos: 4, secs: 3 },
		uid: 7,
	};
	let attr = Server::<TestProvider>::fuse_attr_out(9, attrs);
	let statx = Server::<TestProvider>::fuse_statx_out(attr, 0);

	assert_eq!(statx.stat.mask, libc::STATX_BASIC_STATS);
	assert_eq!(statx.stat.mask & libc::STATX_BTIME, 0);
	assert_eq!(statx.stat.attributes_mask, 0);
	assert_eq!(statx.stat.atime.tv_sec, 1);
	assert_eq!(statx.stat.atime.tv_nsec, 2);
	assert_eq!(statx.stat.mtime.tv_sec, 3);
	assert_eq!(statx.stat.mtime.tv_nsec, 4);
	assert_eq!(statx.stat.ctime.tv_sec, 5);
	assert_eq!(statx.stat.ctime.tv_nsec, 6);
	assert_eq!(statx.stat.size, 513);
	assert_eq!(statx.stat.blocks, 2);
}

#[test]
fn writable_opens_return_erofs() {
	let request = |flags| fuse_open_in {
		flags,
		open_flags: 0,
	};
	Server::<TestProvider>::validate_open_request(request(libc::O_RDONLY.to_u32().unwrap()))
		.unwrap();
	for flags in [libc::O_WRONLY, libc::O_RDWR] {
		let error = Server::<TestProvider>::validate_open_request(request(flags.to_u32().unwrap()))
			.unwrap_err();
		assert_eq!(error.raw_os_error(), Some(libc::EROFS));
	}
}

#[tokio::test]
async fn read_write_dispatcher_handles_concurrency_and_cancellation() {
	let release = Arc::new(tokio::sync::Notify::new());
	let started = Arc::new(tokio::sync::Notify::new());
	let server = server_with_provider(
		SlowProvider {
			release: release.clone(),
			started: started.clone(),
		},
		false,
		false,
	);
	let (response_reader, response_writer) = rustix::net::socketpair(
		AddressFamily::UNIX,
		SocketType::STREAM,
		SocketFlags::CLOEXEC,
		None,
	)
	.unwrap();
	let response_reader = Arc::new(response_reader);
	let response_writer = Arc::new(response_writer);
	let (event_sender, _event_receiver) = tokio::sync::mpsc::unbounded_channel();
	let (request_sender, request_receiver) = tokio::sync::mpsc::channel(4);
	let dispatcher = server.spawn_read_write_dispatcher(request_receiver, event_sender);
	let read = |unique| Request {
		data: RequestData::Read(fuse_read_in {
			fh: 1,
			flags: 0,
			lock_owner: 0,
			offset: 0,
			padding: 0,
			read_flags: 0,
			size: 4,
		}),
		header: request_header(sys::fuse_opcode_FUSE_READ, unique),
	};
	let getattr = |unique| Request {
		data: RequestData::GetAttr,
		header: request_header(sys::fuse_opcode_FUSE_GETATTR, unique),
	};

	let token = server.register_async_request(1).unwrap();
	request_sender
		.send(read_write::ReadWriteRequest {
			fd: response_writer.clone(),
			request: read(1),
			token,
		})
		.await
		.unwrap();
	tokio::time::timeout(Duration::from_secs(2), started.notified())
		.await
		.unwrap();
	let token = server.register_async_request(2).unwrap();
	request_sender
		.send(read_write::ReadWriteRequest {
			fd: response_writer.clone(),
			request: getattr(2),
			token,
		})
		.await
		.unwrap();
	let response = read_response(response_reader.clone()).await;
	assert_eq!(response.unique, 2);
	assert_eq!(response.error, 0);
	release.notify_one();
	let response = read_response(response_reader.clone()).await;
	assert_eq!(response.unique, 1);
	assert_eq!(response.error, 0);

	let token = server.register_async_request(3).unwrap();
	request_sender
		.send(read_write::ReadWriteRequest {
			fd: response_writer.clone(),
			request: read(3),
			token,
		})
		.await
		.unwrap();
	tokio::time::timeout(Duration::from_secs(2), started.notified())
		.await
		.unwrap();
	assert!(server.cancel_async_request(3));
	let response = read_response(response_reader.clone()).await;
	assert_eq!(response.unique, 3);
	assert_eq!(response.error, -libc::EINTR);
	let token = server.register_async_request(4).unwrap();
	request_sender
		.send(read_write::ReadWriteRequest {
			fd: response_writer,
			request: getattr(4),
			token,
		})
		.await
		.unwrap();
	let response = read_response(response_reader).await;
	assert_eq!(response.unique, 4);
	assert_eq!(response.error, 0);

	drop(request_sender);
	dispatcher.await.unwrap();
}

#[test]
fn unmatched_interrupts_return_eagain() {
	let server = server_with_provider(TestProvider, false, false);
	let request = fuse_interrupt_in { unique: 1 };
	let error = server.handle_interrupt_request(request).unwrap_err();
	assert_eq!(error.raw_os_error(), Some(libc::EAGAIN));

	let token = server.register_async_request(1).unwrap();
	let response = server.handle_interrupt_request(request).unwrap();
	assert!(matches!(response, Response::Interrupt));
	assert!(token.is_cancelled());
}

#[test]
fn eventfd_failures_are_reported_to_the_supervisor() {
	let eventfd: OwnedFd = std::fs::File::open("/dev/null").unwrap().into();
	let pending = AtomicBool::new(false);
	let (sender, receiver) = crossbeam_channel::unbounded();
	let (worker_event_sender, mut worker_event_receiver) = tokio::sync::mpsc::unbounded_channel();
	let response = AsyncResponse {
		opcode: sys::fuse_opcode_FUSE_GETATTR,
		result: Err(Error::from_raw_os_error(libc::EIO)),
		slot: 0,
		unique: 1,
	};

	notify_async_response(
		&pending,
		&eventfd,
		response,
		&sender,
		&worker_event_sender,
		0,
	);

	assert!(receiver.try_recv().is_ok());
	assert!(!pending.load(Ordering::Acquire));
	let event = worker_event_receiver.try_recv().unwrap();
	assert!(matches!(event, WorkerEvent::Failed { .. }));
}
