use {
	crate::{Attrs, AttrsInner, EntryKind, Provider, Result},
	dashmap::DashMap,
	std::{
		ffi::{CStr, CString},
		io::{self, Write as _},
		os::fd::{AsRawFd as _, FromRawFd as _, RawFd},
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
		time::Duration,
	},
};

const S_IFDIR: u32 = 0o040_000;
const S_IFREG: u32 = 0o100_000;
const S_IFLNK: u32 = 0o120_000;
const TIMEOUT: Duration = Duration::from_secs(u64::MAX / 2);

#[derive(Clone)]
pub struct Server(Arc<State>);

struct State {
	socket: PathBuf,
	task: Mutex<Option<tangram_futures::task::Shared<()>>>,
}

impl Drop for State {
	fn drop(&mut self) {
		let _ = std::fs::remove_file(&self.socket);
	}
}

pub struct Adapter<P>(P, DashMap<u64, InodeFd>);

struct InodeFd {
	fd: std::fs::File,
	count: usize,
}

pub struct DirIter {
	cursor: usize,
	entries: Vec<Item>,
	offset: u64,
}

struct Item {
	ino: u64,
	type_: u32,
	name: CString,
}

impl Server {
	pub async fn start<P>(provider: P, socket: &Path, dax_window_size: u64) -> Result<Self>
	where
		P: Provider + Send + Sync + 'static,
	{
		let backend = Arc::new(
			virtiofsd::vhost_user::VhostUserFsBackendBuilder::default()
				.set_thread_pool_size(0)
				.set_tag(None)
				.set_dax_window_size(dax_window_size)
				.build(Adapter(provider, DashMap::new()))
				.map_err(|error| {
					io::Error::other(format!("failed to build the vhost-user backend: {error}"))
				})?,
		);
		let mut daemon = vhost_user_backend::VhostUserDaemon::new(
			String::from("tangram-virtiofsd"),
			backend,
			vm_memory::GuestMemoryAtomic::new(vm_memory::GuestMemoryMmap::new()),
		)
		.map_err(|error| {
			io::Error::other(format!("failed to create the vhost-user daemon: {error}"))
		})?;
		let mut listener = vhost::vhost_user::Listener::new(socket, true).map_err(|error| {
			io::Error::other(format!("failed to bind the vhost-user socket: {error:?}"))
		})?;
		let task = tangram_futures::task::Shared::spawn_blocking(move |_stop| {
			if let Err(error) = daemon.start(&mut listener) {
				tracing::error!(?error, "the vhost-user daemon failed to start");
				return;
			}
			if let Err(error) = daemon.wait()
				&& !matches!(
					error,
					vhost_user_backend::Error::HandleRequest(
						vhost::vhost_user::Error::Disconnected,
					),
				) {
				tracing::error!(?error, "the vhost-user daemon exited with an error");
			}
		});
		let state = State {
			socket: socket.to_owned(),
			task: Mutex::new(Some(task)),
		};
		Ok(Self(Arc::new(state)))
	}

	pub fn stop(&self) {
		if let Some(task) = self.0.task.lock().unwrap().as_ref() {
			task.stop();
		}
	}

	pub async fn wait(&self) {
		let task = self.0.task.lock().unwrap().clone();
		if let Some(task) = task {
			let _ = task.wait().await;
		}
	}
}

impl<P> virtiofsd::filesystem::FileSystem for Adapter<P>
where
	P: Provider + Send + Sync,
{
	type Inode = u64;
	type Handle = u64;
	type DirIter = DirIter;

	fn init(&self, capable: virtiofsd::fuse::FsOptions) -> Result<virtiofsd::fuse::FsOptions> {
		let wanted = virtiofsd::fuse::FsOptions::ASYNC_READ
			| virtiofsd::fuse::FsOptions::EXPORT_SUPPORT
			| virtiofsd::fuse::FsOptions::DO_READDIRPLUS;
		Ok(wanted & capable)
	}

	fn lookup(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		parent: u64,
		name: &CStr,
	) -> Result<virtiofsd::filesystem::Entry> {
		let name = name
			.to_str()
			.map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
		let (id, attrs) = self
			.0
			.lookup_and_remember_sync(parent, name)?
			.ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
		Ok(virtiofsd::filesystem::Entry {
			inode: id,
			generation: 0,
			attr: attr_from_attrs(id, attrs),
			attr_timeout: TIMEOUT,
			entry_timeout: TIMEOUT,
		})
	}

	fn forget(&self, _ctx: virtiofsd::filesystem::Context, inode: u64, count: u64) {
		self.0.forget_sync(inode, count);
	}

	fn getattr(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		_handle: Option<u64>,
	) -> Result<(virtiofsd::fuse::Attr, Duration)> {
		let attrs = self.0.getattr_sync(inode)?;
		Ok((attr_from_attrs(inode, attrs), TIMEOUT))
	}

	fn readlink(&self, _ctx: virtiofsd::filesystem::Context, inode: u64) -> Result<Vec<u8>> {
		let bytes = self.0.readlink_sync(inode)?;
		Ok(bytes.to_vec())
	}

	fn open(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		_kill_priv: bool,
		_flags: u32,
	) -> Result<(Option<u64>, virtiofsd::fuse::OpenOptions)> {
		let (handle, backing_fd) = self.0.open_sync(inode)?;
		if let Some(fd) = backing_fd {
			self.1
				.entry(inode)
				.and_modify(|entry| entry.count += 1)
				.or_insert(InodeFd {
					fd: fd.into(),
					count: 1,
				});
		}
		// Artifact content is immutable, so keep the guest page cache across reopens.
		Ok((Some(handle), virtiofsd::fuse::OpenOptions::KEEP_CACHE))
	}

	fn opendir(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		_flags: u32,
	) -> Result<(Option<u64>, virtiofsd::fuse::OpenOptions)> {
		let handle = self.0.opendir_sync(inode)?;
		Ok((Some(handle), virtiofsd::fuse::OpenOptions::empty()))
	}

	fn read<W: virtiofsd::filesystem::ZeroCopyWriter>(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		handle: u64,
		mut w: W,
		size: u32,
		offset: u64,
		_lock_owner: Option<u64>,
		_flags: u32,
	) -> Result<usize> {
		if let Some(entry) = self.1.get(&inode) {
			return w.read_from_file_at(&entry.fd, size as usize, offset, None);
		}

		let bytes = self.0.read_sync(handle, offset, u64::from(size))?;
		if bytes.is_empty() {
			return Ok(0);
		}
		let fd = unsafe { libc::memfd_create(c"tangram-vfs-read".as_ptr(), libc::MFD_CLOEXEC) };
		if fd < 0 {
			return Err(io::Error::last_os_error());
		}
		let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
		file.write_all(&bytes)?;
		w.read_from_file_at(&file, bytes.len(), 0, None)
	}

	fn release(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		_flags: u32,
		handle: u64,
		_flush: bool,
		_flock_release: bool,
		_lock_owner: Option<u64>,
	) -> Result<()> {
		self.1.remove_if_mut(&inode, |_, entry| {
			entry.count -= 1;
			entry.count == 0
		});
		self.0.close_sync(handle);
		Ok(())
	}

	fn releasedir(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		_inode: u64,
		_flags: u32,
		handle: u64,
	) -> Result<()> {
		self.0.close_sync(handle);
		Ok(())
	}

	fn readdir(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		_inode: u64,
		handle: u64,
		size: u32,
		offset: u64,
	) -> Result<DirIter> {
		let entries = self.0.readdir_sync(handle, offset, u64::from(size))?;
		let items = entries
			.into_iter()
			.map(|(name, id, typ)| {
				let name =
					CString::new(name).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
				Ok::<_, io::Error>(Item {
					ino: id,
					type_: dir_entry_type(typ),
					name,
				})
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(DirIter {
			cursor: 0,
			entries: items,
			offset,
		})
	}

	fn getxattr(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		name: &CStr,
		size: u32,
	) -> Result<virtiofsd::filesystem::GetxattrReply> {
		let name = name
			.to_str()
			.map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
		let value = self
			.0
			.getxattr_sync(inode, name)?
			.ok_or_else(|| io::Error::from_raw_os_error(libc::ENODATA))?;
		if size == 0 {
			let count = u32::try_from(value.len()).unwrap_or(u32::MAX);
			return Ok(virtiofsd::filesystem::GetxattrReply::Count(count));
		}
		if value.len() > size as usize {
			return Err(io::Error::from_raw_os_error(libc::ERANGE));
		}
		Ok(virtiofsd::filesystem::GetxattrReply::Value(value.to_vec()))
	}

	fn listxattr(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		size: u32,
	) -> Result<virtiofsd::filesystem::ListxattrReply> {
		let names = self.0.listxattrs_sync(inode)?;
		let mut buf = Vec::new();
		for name in &names {
			buf.extend_from_slice(name.as_bytes());
			buf.push(0);
		}
		if size == 0 {
			let count = u32::try_from(buf.len()).unwrap_or(u32::MAX);
			return Ok(virtiofsd::filesystem::ListxattrReply::Count(count));
		}
		if buf.len() > size as usize {
			return Err(io::Error::from_raw_os_error(libc::ERANGE));
		}
		Ok(virtiofsd::filesystem::ListxattrReply::Names(buf))
	}

	fn access(&self, _ctx: virtiofsd::filesystem::Context, _inode: u64, _mask: u32) -> Result<()> {
		Ok(())
	}

	fn setupmapping(
		&self,
		_ctx: virtiofsd::filesystem::Context,
		inode: u64,
		_handle: u64,
		foffset: u64,
		_len: u64,
		flags: virtiofsd::fuse::SetupmappingFlags,
	) -> Result<(RawFd, u64)> {
		if flags.contains(virtiofsd::fuse::SetupmappingFlags::WRITE) {
			return Err(io::Error::from_raw_os_error(libc::EROFS));
		}
		let Some(entry) = self.1.get(&inode) else {
			return Err(io::Error::from_raw_os_error(libc::EBADF));
		};
		let raw = entry.fd.as_raw_fd();
		Ok((raw, foffset))
	}
}

impl<P> virtiofsd::filesystem::SerializableFileSystem for Adapter<P>
where
	P: Provider + Send + Sync,
{
	fn serialize(&self, _state_pipe: std::fs::File) -> Result<()> {
		Ok(())
	}

	fn deserialize_and_apply(&self, _state_pipe: std::fs::File) -> Result<()> {
		Ok(())
	}
}

impl virtiofsd::filesystem::DirectoryIterator for DirIter {
	fn next(&mut self) -> Option<virtiofsd::filesystem::DirEntry<'_>> {
		let entry = self.entries.get(self.cursor)?;
		self.cursor += 1;
		Some(virtiofsd::filesystem::DirEntry {
			ino: entry.ino as libc::ino64_t,
			offset: self.offset + self.cursor as u64,
			type_: entry.type_,
			name: entry.name.as_c_str(),
		})
	}
}

fn attr_from_attrs(inode: u64, attrs: Attrs) -> virtiofsd::fuse::Attr {
	let (size, mode) = match attrs.inner {
		AttrsInner::Directory => (0, S_IFDIR | 0o555),
		AttrsInner::File { executable, size } => {
			(size, S_IFREG | 0o444 | if executable { 0o111 } else { 0 })
		},
		AttrsInner::Symlink { size } => (size, S_IFLNK | 0o444),
	};
	let blocks = size.div_ceil(512);
	virtiofsd::fuse::Attr {
		ino: inode,
		size,
		blocks,
		atime: attrs.atime.secs,
		mtime: attrs.mtime.secs,
		ctime: attrs.ctime.secs,
		atimensec: attrs.atime.nanos,
		mtimensec: attrs.mtime.nanos,
		ctimensec: attrs.ctime.nanos,
		mode,
		nlink: 1,
		uid: attrs.uid.into(),
		gid: attrs.gid.into(),
		rdev: 0,
		blksize: 512,
		flags: 0,
	}
}

fn dir_entry_type(typ: EntryKind) -> u32 {
	match typ {
		EntryKind::Directory => u32::from(libc::DT_DIR),
		EntryKind::File => u32::from(libc::DT_REG),
		EntryKind::Symlink => u32::from(libc::DT_LNK),
	}
}
