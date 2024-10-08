use self::sys::{
	fuse_attr, fuse_attr_out, fuse_batch_forget_in, fuse_dirent, fuse_direntplus, fuse_entry_out,
	fuse_flush_in, fuse_forget_in, fuse_getattr_in, fuse_getxattr_in, fuse_getxattr_out,
	fuse_in_header, fuse_init_in, fuse_init_out, fuse_open_in, fuse_open_out, fuse_out_header,
	fuse_read_in, fuse_release_in,
};
use crate::{FileType, Provider, Result};
use futures::{future, FutureExt as _};
use num::ToPrimitive as _;
use std::io::Error;
use std::pin::pin;
use std::{
	ffi::CString,
	os::{
		fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
		unix::ffi::OsStrExt,
	},
	path::{Path, PathBuf},
	sync::{Arc, Mutex},
};
use tangram_futures::task::{Stop, Task};
use zerocopy::{FromBytes as _, IntoBytes as _};

pub mod sys;

pub struct Vfs<P>(Arc<Inner<P>>);

pub struct Inner<P> {
	path: PathBuf,
	provider: P,
	task: Mutex<Option<Task<()>>>,
}

/// A request.
#[derive(Clone, Debug)]
struct Request {
	header: sys::fuse_in_header,
	data: RequestData,
}

/// A request's data.
#[derive(Clone, Debug)]
enum RequestData {
	BatchForget(sys::fuse_batch_forget_in),
	Destroy,
	Flush(sys::fuse_flush_in),
	Forget(sys::fuse_forget_in),
	GetAttr(sys::fuse_getattr_in),
	GetXattr(sys::fuse_getxattr_in, CString),
	Init(sys::fuse_init_in),
	ListXattr(sys::fuse_getxattr_in),
	Lookup(CString),
	Open(sys::fuse_open_in),
	OpenDir(sys::fuse_open_in),
	Read(sys::fuse_read_in),
	ReadDir(sys::fuse_read_in),
	ReadDirPlus(sys::fuse_read_in),
	ReadLink,
	Release(sys::fuse_release_in),
	ReleaseDir(sys::fuse_release_in),
	Unsupported(u32),
}

/// A response.
#[derive(Clone, Debug)]
enum Response {
	Flush,
	GetAttr(sys::fuse_attr_out),
	GetXattr(Vec<u8>),
	Init(sys::fuse_init_out),
	ListXattr(Vec<u8>),
	Lookup(sys::fuse_entry_out),
	Open(sys::fuse_open_out),
	OpenDir(sys::fuse_open_out),
	Read(Vec<u8>),
	ReadDir(Vec<u8>),
	ReadDirPlus(Vec<u8>),
	ReadLink(CString),
	Release,
	ReleaseDir,
}

impl<P> Vfs<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub async fn start(provider: P, path: impl AsRef<Path>) -> Result<Self> {
		let path = path.as_ref().to_owned();
		let vfs = Vfs(Arc::new(Inner {
			provider,
			path,
			task: Mutex::new(None),
		}));

		let task = Task::spawn(|stop| {
			let vfs = vfs.clone();
			async move {
				vfs.task(stop).await.ok();
			}
		});
		vfs.task.lock().unwrap().replace(task);
		Ok(vfs)
	}

	pub fn stop(&self) {
		let task = self.task.lock().unwrap();
		if let Some(task) = task.as_ref() {
			task.stop();
		}
	}

	pub async fn wait(self) {
		let task = self.task.lock().unwrap().take();
		if let Some(task) = task {
			task.wait().await.ok();
		}
	}

	async fn task(&self, stop: Stop) -> Result<()> {
		// Unmount.
		self.unmount()
			.await
			.inspect_err(|error| tracing::warn!(%error, "failed to unmount"))
			.ok();

		// Mount.
		let fd = self
			.mount()
			.await
			.inspect_err(|error| tracing::error!("failed to mount {error}"))?;

		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();

		// Create the request channel.
		let (request_sender, request_receiver) = async_channel::bounded(10);

		// Spawn the request task.
		tokio::task::spawn_blocking({
			let fd = fd.clone();
			move || {
				Self::request_task(fd, request_sender).inspect_err(|error| tracing::error!(%error))
			}
		});

		// Handle each request.
		loop {
			let read = request_receiver.recv().map(|r| r.unwrap());
			let stop = stop.stopped();
			let future::Either::Left((request, _)) = future::select(pin!(read), pin!(stop)).await
			else {
				break;
			};

			// Spawn a task to handle the request.
			task_tracker.spawn({
				let server = self.clone();
				let fd = fd.clone();
				async move {
					// Get the unique identifier for the request.
					let unique = request.header.unique;

					// Handle the request.
					let result = server.handle_request(request).await.inspect_err(|error| {
						tracing::error!(?error, "request failed");
					});

					// Write the response.
					match result {
						Ok(Some(response)) => {
							write_response(fd.as_raw_fd(), unique, &response)
								.inspect_err(|error| {
									tracing::error!(?error, "failed to write the response");
								})
								.ok();
						},
						Ok(None) => (),
						Err(error) => {
							let error = error.raw_os_error().unwrap_or(libc::ENOSYS);
							write_error(fd.as_raw_fd(), unique, error)
								.inspect_err(|error| {
									tracing::error!(?error, "failed to write the error");
								})
								.ok();
						},
					}
				}
			});
		}

		// Wait for all tasks to complete.
		task_tracker.close();
		task_tracker.wait().await;

		// Unmount.
		self.unmount()
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to unmount"))
			.ok();

		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn request_task(fd: Arc<OwnedFd>, sender: async_channel::Sender<Request>) -> Result<()> {
		// Create the request buffer.
		let mut buffer = vec![0u8; 1024 * 1024 + 4096];

		loop {
			// Read.
			let n = unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };

			// Handle a read error.
			if n < 0 {
				let error = std::io::Error::last_os_error();
				if matches!(
					error.raw_os_error(),
					Some(libc::ENOENT | libc::EINTR | libc::EAGAIN)
				) {
					continue;
				} else if matches!(error.raw_os_error(), Some(libc::ENODEV)) {
					return Ok(());
				}
				return Err(error);
			}
			let n = n.to_usize().unwrap();

			// Copy the request.
			let bytes = &buffer[..n];

			// Deserialize the request.
			let request = Self::deserialize_request(bytes)?;

			// Send the request.
			let result = sender.send_blocking(request);
			if result.is_err() {
				return Ok(());
			}
		}
	}

	fn deserialize_request(buffer: &[u8]) -> Result<Request> {
		let (header, _) = sys::fuse_in_header::read_from_prefix(buffer)
			.map_err(|_| Error::other("failed to deserialize the request header"))?;
		let header_len = std::mem::size_of::<sys::fuse_in_header>();
		let data = &buffer[header_len..];
		let data = match header.opcode {
			sys::fuse_opcode::FUSE_BATCH_FORGET => RequestData::BatchForget(read_data(data)?),
			sys::fuse_opcode::FUSE_DESTROY => RequestData::Destroy,
			sys::fuse_opcode::FUSE_FLUSH => RequestData::Flush(read_data(data)?),
			sys::fuse_opcode::FUSE_FORGET => RequestData::Forget(read_data(data)?),
			sys::fuse_opcode::FUSE_GETATTR => RequestData::GetAttr(read_data(data)?),
			sys::fuse_opcode::FUSE_GETXATTR => {
				let (fuse_getxattr_in, name) =
					data.split_at(std::mem::size_of::<sys::fuse_getxattr_in>());
				let fuse_getxattr_in = read_data(fuse_getxattr_in)?;
				let name = CString::from_vec_with_nul(name.to_owned())
					.map_err(|_| Error::other("failed to deserialize request data"))?;
				RequestData::GetXattr(fuse_getxattr_in, name)
			},
			sys::fuse_opcode::FUSE_INIT => RequestData::Init(read_data(data)?),
			sys::fuse_opcode::FUSE_LISTXATTR => RequestData::ListXattr(read_data(data)?),
			sys::fuse_opcode::FUSE_LOOKUP => {
				let data = CString::from_vec_with_nul(data.to_owned())
					.map_err(|_| Error::other("failed to deserialize request data"))?;
				RequestData::Lookup(data)
			},
			sys::fuse_opcode::FUSE_OPEN => RequestData::Open(read_data(data)?),
			sys::fuse_opcode::FUSE_OPENDIR => RequestData::OpenDir(read_data(data)?),
			sys::fuse_opcode::FUSE_READ => RequestData::Read(read_data(data)?),
			sys::fuse_opcode::FUSE_READDIR => RequestData::ReadDir(read_data(data)?),
			sys::fuse_opcode::FUSE_READDIRPLUS => RequestData::ReadDirPlus(read_data(data)?),
			sys::fuse_opcode::FUSE_READLINK => RequestData::ReadLink,
			sys::fuse_opcode::FUSE_RELEASE => RequestData::Release(read_data(data)?),
			sys::fuse_opcode::FUSE_RELEASEDIR => RequestData::ReleaseDir(read_data(data)?),
			_ => RequestData::Unsupported(header.opcode),
		};
		let request = Request { header, data };
		Ok(request)
	}

	async fn handle_request(&self, request: Request) -> Result<Option<Response>> {
		match request.data {
			RequestData::BatchForget(data) => {
				self.handle_batch_forget_request(request.header, data).await
			},
			RequestData::Destroy => Ok(None),
			RequestData::Flush(data) => self.handle_flush_request(request.header, data).await,
			RequestData::Forget(data) => self.handle_forget_request(request.header, data).await,
			RequestData::GetAttr(data) => self.handle_get_attr_request(request.header, data).await,
			RequestData::GetXattr(data, name) => {
				self.handle_get_xattr_request(request.header, data, name)
					.await
			},
			RequestData::Init(data) => self.handle_init_request(request.header, data).await,
			RequestData::ListXattr(data) => {
				self.handle_list_xattr_request(request.header, data).await
			},
			RequestData::Lookup(data) => self.handle_lookup_request(request.header, data).await,
			RequestData::Open(data) => self.handle_open_request(request.header, data).await,
			RequestData::OpenDir(data) => self.handle_open_dir_request(request.header, data).await,
			RequestData::Read(data) => self.handle_read_request(request.header, data).await,
			RequestData::ReadDir(data) => {
				self.handle_read_dir_request(request.header, data, false)
					.await
			},
			RequestData::ReadDirPlus(data) => {
				self.handle_read_dir_request(request.header, data, true)
					.await
			},
			RequestData::ReadLink => self.handle_read_link_request(request.header).await,
			RequestData::Release(data) => self.handle_release_request(request.header, data).await,
			RequestData::ReleaseDir(data) => {
				self.handle_release_dir_request(request.header, data).await
			},
			RequestData::Unsupported(opcode) => {
				self.handle_unsupported_request(request.header, opcode)
					.await
			},
		}
	}

	async fn handle_batch_forget_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_batch_forget_in,
	) -> Result<Option<Response>> {
		Ok(None)
	}

	async fn handle_flush_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_flush_in,
	) -> Result<Option<Response>> {
		Ok(Some(Response::Flush))
	}

	async fn handle_forget_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_forget_in,
	) -> Result<Option<Response>> {
		Ok(None)
	}

	async fn handle_get_attr_request(
		&self,
		header: fuse_in_header,
		_request: fuse_getattr_in,
	) -> Result<Option<Response>> {
		let out = self.fuse_attr_out(header.nodeid).await?;
		Ok(Some(Response::GetAttr(out)))
	}

	async fn handle_get_xattr_request(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
		name: CString,
	) -> Result<Option<Response>> {
		let name = name
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::ENODATA))?;
		let attr = self
			.provider
			.getxattr(header.nodeid, name)
			.await?
			.ok_or_else(|| Error::from_raw_os_error(libc::ENODATA))?;

		// If the request size is 0, the driver is requesting the size of the xattr.
		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attr.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::GetXattr(response)))
		} else if request.size.to_usize().unwrap() < attr.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::GetXattr(attr.into())))
		}
	}

	async fn handle_init_request(
		&self,
		_header: fuse_in_header,
		request: fuse_init_in,
	) -> Result<Option<Response>> {
		let response = fuse_init_out {
			major: 7,
			minor: 21,
			max_readahead: request.max_readahead,
			flags: sys::FUSE_DO_READDIRPLUS,
			max_background: 0,
			congestion_threshold: 0,
			max_write: 1024 * 1024,
			time_gran: 0,
			max_pages: 0,
			map_alignment: 0,
			flags2: 0,
			unused: [0; 7],
		};
		Ok(Some(Response::Init(response)))
	}

	async fn handle_list_xattr_request(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
	) -> Result<Option<Response>> {
		let attrs = self
			.provider
			.listxattrs(header.nodeid)
			.await?
			.into_iter()
			.flat_map(|s| {
				let mut s = s.into_bytes();
				s.push(0);
				s.into_iter()
			})
			.collect::<Vec<_>>();

		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attrs.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::ListXattr(response)))
		} else if request.size.to_usize().unwrap() < attrs.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::ListXattr(attrs)))
		}
	}

	async fn handle_lookup_request(
		&self,
		header: fuse_in_header,
		request: CString,
	) -> Result<Option<Response>> {
		let name = request
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::EINVAL))?;
		let node = self
			.provider
			.lookup(header.nodeid, name)
			.await?
			.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
		let out = self.fuse_entry_out(node).await?;
		Ok(Some(Response::Lookup(out)))
	}

	async fn handle_open_request(
		&self,
		header: fuse_in_header,
		_request: fuse_open_in,
	) -> Result<Option<Response>> {
		let fh = self.provider.open(header.nodeid).await?;
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_NOFLUSH | sys::FOPEN_KEEP_CACHE,
			padding: 0,
		};
		Ok(Some(Response::Open(out)))
	}

	async fn handle_open_dir_request(
		&self,
		header: fuse_in_header,
		_request: fuse_open_in,
	) -> Result<Option<Response>> {
		let fh = self.provider.opendir(header.nodeid).await?;
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE,
			padding: 0,
		};
		Ok(Some(Response::OpenDir(out)))
	}

	async fn handle_read_request(
		&self,
		_header: fuse_in_header,
		request: fuse_read_in,
	) -> Result<Option<Response>> {
		let bytes = self
			.provider
			.read(request.fh, request.offset, request.size.to_u64().unwrap())
			.await?;
		Ok(Some(Response::Read(bytes.to_vec())))
	}

	async fn handle_read_dir_request(
		&self,
		_header: fuse_in_header,
		request: fuse_read_in,
		plus: bool,
	) -> Result<Option<Response>> {
		let entries = self.provider.readdir(request.fh).await?;

		let struct_size = if plus {
			std::mem::size_of::<fuse_direntplus>()
		} else {
			std::mem::size_of::<fuse_dirent>()
		};

		let entries = entries
			.into_iter()
			.enumerate()
			.skip(request.offset.to_usize().unwrap());
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		for (offset, (name, node)) in entries {
			let attr = self.provider.getattr(node).await?;
			let name = name.into_bytes();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

			let type_ = match attr.typ {
				FileType::Directory => libc::S_IFDIR.to_u32().unwrap(),
				FileType::File { .. } => libc::S_IFREG.to_u32().unwrap(),
				FileType::Symlink => libc::S_IFLNK.to_u32().unwrap(),
			};

			let entry = fuse_dirent {
				ino: node,
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_,
			};

			if plus {
				let entry = fuse_direntplus {
					entry_out: self.fuse_entry_out(node).await?,
					dirent: entry,
				};
				response.extend_from_slice(entry.as_bytes());
			} else {
				response.extend_from_slice(entry.as_bytes());
			}
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}

		if plus {
			Ok(Some(Response::ReadDirPlus(response)))
		} else {
			Ok(Some(Response::ReadDir(response)))
		}
	}

	async fn handle_read_link_request(&self, header: fuse_in_header) -> Result<Option<Response>> {
		let target = self.provider.readlink(header.nodeid).await?;
		let target = CString::new(target.as_bytes()).unwrap();
		Ok(Some(Response::ReadLink(target)))
	}

	async fn handle_release_request(
		&self,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Result<Option<Response>> {
		self.provider.close(request.fh).await;
		Ok(Some(Response::Release))
	}

	async fn handle_release_dir_request(
		&self,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Result<Option<Response>> {
		self.provider.close(request.fh).await;
		Ok(Some(Response::ReleaseDir))
	}

	async fn handle_unsupported_request(
		&self,
		header: fuse_in_header,
		request: u32,
	) -> Result<Option<Response>> {
		tracing::error!(?header, %request, "unsupported request");
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	async fn mount(&self) -> Result<Arc<OwnedFd>> {
		let path = &self.path;

		unsafe {
			// Create the file socket pair.
			let mut fds = [0, 0];
			let ret = libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr());
			if ret != 0 {
				return Err(std::io::Error::last_os_error());
			}
			let fuse_commfd = CString::new(fds[0].to_string()).unwrap();

			// Create the args.
			let uid = libc::getuid();
			let gid = libc::getgid();
			let options = CString::new(format!(
				"rootmode=40755,user_id={uid},group_id={gid},default_permissions"
			))
			.unwrap();
			let path = CString::new(path.as_os_str().as_bytes()).unwrap();

			// Fork.
			let pid = libc::fork();
			if pid == -1 {
				libc::close(fds[0]);
				libc::close(fds[1]);
				let source = std::io::Error::last_os_error();
				return Err(source);
			}

			// Exec.
			if pid == 0 {
				let argv = [
					c"fusermount3".as_ptr(),
					c"-o".as_ptr(),
					options.as_ptr(),
					c"--".as_ptr(),
					path.as_ptr(),
					std::ptr::null(),
				];
				libc::close(fds[1]);
				libc::fcntl(fds[0], libc::F_SETFD, 0);
				libc::setenv(c"_FUSE_COMMFD".as_ptr(), fuse_commfd.as_ptr(), 1);
				libc::execvp(argv[0], argv.as_ptr());
				libc::close(fds[0]);
				libc::exit(1);
			}
			libc::close(fds[0]);

			// Create the control message.
			let mut control = vec![0u8; libc::CMSG_SPACE(4) as usize];
			let mut buffer = vec![0u8; 8];
			let mut msg = Box::new(libc::msghdr {
				msg_name: std::ptr::null_mut(),
				msg_namelen: 0,
				msg_iov: [libc::iovec {
					iov_base: buffer.as_mut_ptr().cast(),
					iov_len: buffer.len(),
				}]
				.as_mut_ptr(),
				msg_iovlen: 1,
				msg_control: control.as_mut_ptr().cast(),
				#[allow(clippy::cast_possible_truncation)]
				msg_controllen: std::mem::size_of_val(&control) as _,
				msg_flags: 0,
			});
			let msg: *mut libc::msghdr = msg.as_mut() as _;

			// Receive the control message.
			let ret = libc::recvmsg(fds[1], msg, 0);
			if ret == -1 {
				return Err(Error::last_os_error());
			}
			if ret == 0 {
				return Err(Error::other("failed to read the control message"));
			}

			// Get the file descriptor.
			let cmsg = libc::CMSG_FIRSTHDR(msg);
			if cmsg.is_null() {
				return Err(Error::other("missing control message"));
			}
			let mut fd: RawFd = 0;
			libc::memcpy(
				std::ptr::addr_of_mut!(fd).cast(),
				libc::CMSG_DATA(cmsg).cast(),
				std::mem::size_of_val(&fd),
			);
			let ret = libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
			if ret == -1 {
				return Err(std::io::Error::last_os_error());
			}
			let fd = Arc::new(OwnedFd::from_raw_fd(fd));

			// Reap the process.
			let ret = libc::waitpid(pid, std::ptr::null_mut(), 0);
			if ret == -1 {
				return Err(std::io::Error::last_os_error());
			}

			Ok(fd)
		}
	}

	async fn unmount(&self) -> Result<()> {
		let path = &self.path;
		tokio::process::Command::new("fusermount3")
			.args(["-u"])
			.arg(path)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.await?;
		Ok(())
	}

	async fn fuse_attr_out(&self, node: u64) -> Result<fuse_attr_out> {
		let attr = self.provider.getattr(node).await?;
		let (size, mode) = match attr.typ {
			FileType::Directory => (0, libc::S_IFDIR | 0o555),
			FileType::File { executable, size } => (
				size,
				libc::S_IFREG | 0o444 | (if executable { 0o111 } else { 0o000 }),
			),
			FileType::Symlink => (0, libc::S_IFLNK | 0o444),
		};
		let mode = mode.to_u32().unwrap();
		let attr_out = fuse_attr_out {
			attr_valid: 1024,
			attr_valid_nsec: 0,
			attr: fuse_attr {
				ino: node,
				size,
				blocks: 0,
				atime: attr.atime.secs,
				atimensec: attr.atime.nanos,
				mtime: attr.mtime.secs,
				mtimensec: attr.mtime.nanos,
				ctime: attr.ctime.secs,
				ctimensec: attr.ctime.nanos,
				mode,
				nlink: 1,
				uid: attr.uid,
				gid: attr.gid,
				rdev: 0,
				blksize: 512,
				flags: 0,
			},
			dummy: 0,
		};

		Ok(attr_out)
	}

	async fn fuse_entry_out(&self, node: u64) -> Result<fuse_entry_out> {
		let attr_out = self.fuse_attr_out(node).await?;
		let entry_out = fuse_entry_out {
			nodeid: node,
			generation: 0,
			entry_valid: 1024,
			attr_valid: 0,
			entry_valid_nsec: 1024,
			attr_valid_nsec: 0,
			attr: attr_out.attr,
		};
		Ok(entry_out)
	}
}

fn read_data<T>(request_data: &[u8]) -> Result<T>
where
	T: zerocopy::FromBytes,
{
	T::read_from_prefix(request_data)
		.map(|(data, _)| data)
		.map_err(|_| Error::other("failed to deserialize the request data"))
}

fn write_error(fd: RawFd, unique: u64, error: i32) -> std::io::Result<()> {
	let len = std::mem::size_of::<fuse_out_header>();
	let header = fuse_out_header {
		unique,
		len: len.to_u32().unwrap(),
		error: -error,
	};
	let header = header.as_bytes();
	let iov = [libc::iovec {
		iov_base: header.as_ptr() as *mut _,
		iov_len: header.len(),
	}];
	let ret = unsafe { libc::writev(fd.as_raw_fd(), iov.as_ptr(), 1) };
	if ret == -1 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

fn write_response(fd: RawFd, unique: u64, response: &Response) -> std::io::Result<()> {
	let data = match response {
		Response::Flush | Response::Release | Response::ReleaseDir => &[],
		Response::GetAttr(data) => data.as_bytes(),
		Response::Init(data) => data.as_bytes(),
		Response::Lookup(data) => data.as_bytes(),
		Response::Open(data) | Response::OpenDir(data) => data.as_bytes(),
		Response::Read(data)
		| Response::ReadDir(data)
		| Response::ReadDirPlus(data)
		| Response::GetXattr(data)
		| Response::ListXattr(data) => data.as_bytes(),
		Response::ReadLink(data) => data.as_bytes(),
	};
	let len = std::mem::size_of::<fuse_out_header>() + data.len();
	let header = fuse_out_header {
		unique,
		len: len.to_u32().unwrap(),
		error: 0,
	};
	let header = header.as_bytes();
	let iov = [
		libc::iovec {
			iov_base: header.as_ptr() as *mut _,
			iov_len: header.len(),
		},
		libc::iovec {
			iov_base: data.as_ptr() as *mut _,
			iov_len: data.len(),
		},
	];
	let ret = unsafe { libc::writev(fd.as_raw_fd(), iov.as_ptr(), 2) };
	if ret == -1 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

impl<P> Clone for Vfs<P> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<P> std::ops::Deref for Vfs<P> {
	type Target = Inner<P>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
