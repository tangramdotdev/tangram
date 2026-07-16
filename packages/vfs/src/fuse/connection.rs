use super::*;

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) async fn mount(path: &Path) -> Result<Arc<OwnedFd>> {
		let (fuse_commfd, recvfd) = rustix::net::socketpair(
			AddressFamily::UNIX,
			SocketType::STREAM,
			SocketFlags::CLOEXEC,
			None,
		)
		.map_err(Error::from)?;

		let commfd_raw = fuse_commfd.as_raw_fd();
		let uid = rustix::process::getuid().as_raw();
		let gid = rustix::process::getgid().as_raw();
		let options = format!("rootmode=40755,user_id={uid},group_id={gid},default_permissions,ro");
		// SAFETY: The pre_exec closure only calls async-signal-safe operations.
		let mut child = unsafe {
			std::process::Command::new("fusermount3")
				.args(["-o", &options, "--"])
				.arg(path)
				.env("_FUSE_COMMFD", commfd_raw.to_string())
				.stdin(std::process::Stdio::null())
				.stdout(std::process::Stdio::null())
				.stderr(std::process::Stdio::null())
				.pre_exec(move || {
					// Clear CLOEXEC on the comm fd so fusermount3 inherits it.
					rustix::io::fcntl_setfd(&fuse_commfd, FdFlags::empty()).map_err(Error::from)?;
					Ok(())
				})
				.spawn()?
		};

		let mut read_buffer = [0u8; 8];
		let mut iovecs = [IoSliceMut::new(&mut read_buffer)];
		let mut cmsg_space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(1))];
		let mut cmsg_buffer = RecvAncillaryBuffer::new(&mut cmsg_space);
		let recv = rustix::net::recvmsg(&recvfd, &mut iovecs, &mut cmsg_buffer, RecvFlags::empty())
			.map_err(Error::from)?;
		if recv.bytes == 0 {
			return Err(Error::other("failed to read the control message"));
		}

		let mut fd = None;
		for message in cmsg_buffer.drain() {
			if let RecvAncillaryMessage::ScmRights(mut fds) = message {
				fd = fds.next();
				if fd.is_some() {
					break;
				}
			}
		}
		let fd = fd.ok_or_else(|| Error::other("missing control message"))?;
		rustix::io::fcntl_setfd(&fd, FdFlags::CLOEXEC).map_err(Error::from)?;

		child.wait()?;

		Ok(Arc::new(fd))
	}

	pub async fn unmount(path: &Path) -> Result<()> {
		let output = tokio::process::Command::new("fusermount3")
			.args(["-u", "-z"])
			.arg(path)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::piped())
			.output()
			.await?;
		if !output.status.success() {
			let stderr = String::from_utf8_lossy(&output.stderr);
			return Err(Error::other(format!("failed to unmount: {stderr}")));
		}
		Ok(())
	}

	pub(super) async fn disconnect_transport(path: &Path, connection_id: u64) -> bool {
		let aborted = Self::abort_connection(connection_id)
			.inspect_err(|error| tracing::error!(%error, "failed to abort the FUSE connection"))
			.is_ok();
		Self::unmount(path)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to unmount"))
			.ok();

		aborted
	}

	pub(super) fn abort_connection(connection_id: u64) -> Result<()> {
		let abort = Path::new("/sys/fs/fuse/connections")
			.join(connection_id.to_string())
			.join("abort");
		std::fs::write(abort, b"1")?;

		Ok(())
	}

	pub(super) fn connection_id(path: &Path) -> Result<u64> {
		let path = if path.is_absolute() {
			path.to_owned()
		} else {
			std::env::current_dir()?.join(path)
		};
		let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;

		Self::parse_connection_id(&mountinfo, &path)
	}

	pub(super) fn parse_connection_id(mountinfo: &str, path: &Path) -> Result<u64> {
		for line in mountinfo.lines().rev() {
			let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
			let Some(separator) = fields.iter().position(|field| *field == "-") else {
				continue;
			};
			if fields.len() <= separator + 1 || !fields[separator + 1].starts_with("fuse") {
				continue;
			}
			let Some(mountpoint) = fields.get(4) else {
				continue;
			};
			if Self::unescape_mountinfo_path(mountpoint) != path {
				continue;
			}
			let Some((major, minor)) = fields.get(2).and_then(|device| device.split_once(':'))
			else {
				continue;
			};
			let major = major.parse::<u32>().map_err(|error| {
				Error::other(format!(
					"failed to parse the FUSE device major number: {error}"
				))
			})?;
			let minor = minor.parse::<u32>().map_err(|error| {
				Error::other(format!(
					"failed to parse the FUSE device minor number: {error}"
				))
			})?;
			let connection_id = libc::makedev(major, minor);

			return Ok(connection_id);
		}

		Err(Error::other(
			"failed to find the FUSE connection in mountinfo",
		))
	}

	pub(super) fn unescape_mountinfo_path(path: &str) -> std::path::PathBuf {
		let bytes = path.as_bytes();
		let mut output = Vec::with_capacity(bytes.len());
		let mut index = 0;
		while index < bytes.len() {
			if bytes[index] == b'\\'
				&& index + 3 < bytes.len()
				&& bytes[index + 1..=index + 3]
					.iter()
					.all(|byte| matches!(byte, b'0'..=b'7'))
			{
				let value = (bytes[index + 1] - b'0') * 64
					+ (bytes[index + 2] - b'0') * 8
					+ bytes[index + 3]
					- b'0';
				output.push(value);
				index += 4;
			} else {
				output.push(bytes[index]);
				index += 1;
			}
		}

		OsString::from_vec(output).into()
	}

	pub(super) fn join_transport_threads(
		thread_handles: &mut Vec<std::thread::JoinHandle<()>>,
		disconnected: bool,
	) {
		if !disconnected {
			tracing::error!(
				"detaching FUSE workers because the connection could not be terminated"
			);
			thread_handles.clear();
			return;
		}
		for thread_handle in thread_handles.drain(..) {
			thread_handle.join().ok();
		}
	}

	pub fn stop(&self) {
		self.task.lock().unwrap().as_ref().unwrap().stop();
	}

	pub async fn wait(&self) {
		let task = self.task.lock().unwrap().clone().unwrap();
		task.wait().await.unwrap();
	}

	pub(super) fn init_handshake(
		fd: &OwnedFd,
		options: Options,
		limits: RequestLimits,
		supports_no_opendir: bool,
	) -> Result<Features> {
		const FUSE_PASSTHROUGH_FLAGS2: u32 = (sys::FUSE_PASSTHROUGH >> 32) as u32;
		const FUSE_OVER_IO_URING_FLAGS2: u32 = (sys::FUSE_OVER_IO_URING >> 32) as u32;
		const REQUIRED_FLAGS: u32 = 0;
		const NEGOTIATED_FLAGS: u32 = sys::FUSE_ASYNC_READ
			| sys::FUSE_DO_READDIRPLUS
			| sys::FUSE_READDIRPLUS_AUTO
			| sys::FUSE_PARALLEL_DIROPS
			| sys::FUSE_CACHE_SYMLINKS
			| sys::FUSE_SPLICE_MOVE
			| sys::FUSE_SPLICE_READ
			| sys::FUSE_MAX_PAGES
			| sys::FUSE_MAP_ALIGNMENT
			| sys::FUSE_INIT_EXT;
		const MAP_ALIGNMENT: u16 = 12;
		// The kernel requires a non-zero max_stack_depth to enable FUSE_PASSTHROUGH.
		const MAX_STACK_DEPTH: u32 = 2;
		let mut negotiated_flags = NEGOTIATED_FLAGS;
		if supports_no_opendir {
			negotiated_flags |= sys::FUSE_NO_OPENDIR_SUPPORT;
		}
		let mut required_flags2 = 0;
		if options.io == Io::IoUring {
			required_flags2 |= FUSE_OVER_IO_URING_FLAGS2;
		}
		if options.passthrough == Passthrough::Required {
			required_flags2 |= FUSE_PASSTHROUGH_FLAGS2;
		}
		let mut negotiated_flags2 = 0;
		if options.io != Io::ReadWrite {
			negotiated_flags2 |= FUSE_OVER_IO_URING_FLAGS2;
		}
		if options.passthrough != Passthrough::Disabled {
			negotiated_flags2 |= FUSE_PASSTHROUGH_FLAGS2;
		}

		let mut buffer = vec![0u8; limits.request_buffer_size];
		loop {
			let ret = match rustix::io::read(fd, &mut buffer) {
				Ok(ret) => ret,
				Err(Errno::INTR) => continue,
				Err(error) => return Err(error.into()),
			};
			if ret == 0 {
				return Err(Error::other("failed to read init request"));
			}
			let size = ret.to_usize().unwrap();
			let request = Self::deserialize_request(&buffer[..size])?;
			let RequestData::Init(init) = request.data else {
				return Err(Error::other("expected init request"));
			};

			if init.major != FUSE_KERNEL_VERSION {
				return Err(Error::other("unsupported fuse major version"));
			}
			let init_ext_available = (init.flags & sys::FUSE_INIT_EXT) != 0;
			let init_flags2 = if init_ext_available { init.flags2 } else { 0 };
			let missing_flags = REQUIRED_FLAGS & !init.flags;
			if missing_flags != 0 {
				return Err(Error::other(format!(
					"missing required fuse init flags, missing = {missing_flags:#x}",
				)));
			}
			let missing_flags2 = required_flags2 & !init_flags2;
			if missing_flags2 != 0 {
				return Err(Error::other(format!(
					"missing required fuse init flags2, missing = {missing_flags2:#x}",
				)));
			}

			let minor = init.minor.min(FUSE_KERNEL_MINOR_VERSION);
			let flags = negotiated_flags & init.flags;
			let flags2 = negotiated_flags2 & init_flags2;
			let max_pages = if (flags & sys::FUSE_MAX_PAGES) != 0 {
				limits.max_pages
			} else {
				0
			};
			let map_alignment = if (flags & sys::FUSE_MAP_ALIGNMENT) != 0 {
				MAP_ALIGNMENT
			} else {
				0
			};
			let max_stack_depth = if (flags2 & FUSE_PASSTHROUGH_FLAGS2) != 0 {
				MAX_STACK_DEPTH
			} else {
				0
			};
			let response = fuse_init_out {
				major: FUSE_KERNEL_VERSION,
				minor,
				max_readahead: limits.max_write,
				flags,
				max_background: 0,
				congestion_threshold: 0,
				max_write: limits.max_write,
				time_gran: 0,
				max_pages,
				map_alignment,
				flags2,
				max_stack_depth,
				request_timeout: 0,
				unused: [0; 11],
			};

			let data = &response.as_bytes()[..Self::init_response_size(minor)];
			let len = size_of::<fuse_out_header>() + data.len();
			let header = fuse_out_header {
				unique: request.header.unique,
				len: len.to_u32().unwrap(),
				error: 0,
			};
			let header = header.as_bytes();
			let iov = [IoSlice::new(header), IoSlice::new(data)];
			rustix::io::writev(fd, &iov).map_err(std::io::Error::from)?;
			return Ok(Features {
				no_opendir_support: (flags & sys::FUSE_NO_OPENDIR_SUPPORT) != 0,
				over_io_uring: (flags2 & FUSE_OVER_IO_URING_FLAGS2) != 0,
				passthrough: (flags2 & FUSE_PASSTHROUGH_FLAGS2) != 0,
			});
		}
	}

	pub(super) fn init_response_size(minor: u32) -> usize {
		if minor < 5 {
			sys::FUSE_COMPAT_INIT_OUT_SIZE.to_usize().unwrap()
		} else if minor < 23 {
			sys::FUSE_COMPAT_22_INIT_OUT_SIZE.to_usize().unwrap()
		} else {
			size_of::<fuse_init_out>()
		}
	}

	pub(super) fn is_clone_not_supported_error(error: &Error) -> bool {
		matches!(
			error.raw_os_error(),
			Some(libc::ENOSYS | libc::ENOTTY | libc::EINVAL)
		)
	}

	pub(super) fn clone_thread_fd(fd: &OwnedFd) -> Result<OwnedFd> {
		let cloned_file = std::fs::File::options()
			.read(true)
			.write(true)
			.open("/dev/fuse")?;
		let cloned: OwnedFd = cloned_file.into();
		rustix::io::fcntl_setfd(&cloned, FdFlags::CLOEXEC).map_err(Error::from)?;
		let mut source_fd = fd
			.as_raw_fd()
			.to_u32()
			.ok_or_else(|| Error::other("invalid source fuse fd"))?;
		// SAFETY: The ioctl receives a valid pointer to the source descriptor number, and both
		// descriptors remain open for the duration of the call.
		unsafe {
			ioctl::ioctl(
				&cloned,
				IoctlPointerInt::<FUSE_DEV_IOC_CLONE, _>::new(&mut source_fd),
			)
		}
		.map_err(Error::from)?;
		Ok(cloned)
	}

	pub(super) fn clone_read_write_fds(fd: &Arc<OwnedFd>) -> Result<Vec<Arc<OwnedFd>>> {
		let reader_count = std::thread::available_parallelism()
			.map_or(1, std::num::NonZero::get)
			.clamp(1, READ_WRITE_MAX_READER_COUNT);
		let mut fds = Vec::with_capacity(reader_count);
		fds.push(fd.clone());
		for reader_id in 1..reader_count {
			match Self::clone_thread_fd(fd) {
				Ok(fd) => fds.push(Arc::new(fd)),
				Err(error) if Self::is_clone_not_supported_error(&error) => {
					tracing::warn!(
						?error,
						%reader_id,
						"FUSE fd cloning is not supported; using the available ReadWrite readers",
					);
					break;
				},
				Err(error) => {
					return Err(Error::other(format!(
						"failed to clone the ReadWrite reader fd {reader_id}: {error}",
					)));
				},
			}
		}

		Ok(fds)
	}
}
