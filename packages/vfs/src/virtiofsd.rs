use {
	crate::{Attrs, DirEntryType, FileType, Provider, Result},
	std::{
		ffi::{CStr, CString},
		io::{self, Write as _},
		os::fd::FromRawFd as _,
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
		time::Duration,
	},
	virtiofsd::{
		filesystem::{
			Context, DirEntry, DirectoryIterator, Entry, FileSystem, GetxattrReply,
			ListxattrReply, SerializableFileSystem, ZeroCopyWriter,
		},
		fuse::{self, FsOptions, OpenOptions},
		vhost_user::VhostUserFsBackendBuilder,
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

pub struct Adapter<P>(P);

pub struct DirIter {
	entries: Vec<Item>,
	cursor: usize,
}

struct Item {
	ino: u64,
	type_: u32,
	name: CString,
}

impl Server {
	pub async fn start<P>(provider: P, socket: &Path) -> Result<Self>
	where
		P: Provider + Send + Sync + 'static,
	{
		let backend = Arc::new(
			VhostUserFsBackendBuilder::default()
				.set_thread_pool_size(0)
				.set_tag(None)
				.build(Adapter(provider))
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
		let listener = vhost::vhost_user::Listener::new(socket, true).map_err(|error| {
			io::Error::other(format!("failed to bind the vhost-user socket: {error:?}"))
		})?;
		// `daemon.start(listener)` blocks accepting the first vhost-user connection, so move it
		// and `daemon.wait()` into the blocking task and return immediately.
		let task = tangram_futures::task::Shared::spawn_blocking(move |_stop| {
			if let Err(error) = daemon.start(listener) {
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

impl<P> FileSystem for Adapter<P>
where
	P: Provider + Send + Sync,
{
	type Inode = u64;
	type Handle = u64;
	type DirIter = DirIter;

	fn init(&self, capable: FsOptions) -> Result<FsOptions> {
		let wanted = FsOptions::ASYNC_READ | FsOptions::EXPORT_SUPPORT | FsOptions::DO_READDIRPLUS;
		Ok(wanted & capable)
	}

	fn lookup(&self, _ctx: Context, parent: u64, name: &CStr) -> Result<Entry> {
		let name = name
			.to_str()
			.map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
		let id = self
			.0
			.lookup_sync(parent, name)?
			.ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
		let attrs = self.0.getattr_sync(id)?;
		self.0.remember_sync(id);
		Ok(Entry {
			inode: id,
			generation: 0,
			attr: attr_from_attrs(id, attrs),
			attr_timeout: TIMEOUT,
			entry_timeout: TIMEOUT,
		})
	}

	fn forget(&self, _ctx: Context, inode: u64, count: u64) {
		self.0.forget_sync(inode, count);
	}

	fn getattr(
		&self,
		_ctx: Context,
		inode: u64,
		_handle: Option<u64>,
	) -> Result<(fuse::Attr, Duration)> {
		let attrs = self.0.getattr_sync(inode)?;
		Ok((attr_from_attrs(inode, attrs), TIMEOUT))
	}

	fn readlink(&self, _ctx: Context, inode: u64) -> Result<Vec<u8>> {
		let bytes = self.0.readlink_sync(inode)?;
		Ok(bytes.to_vec())
	}

	fn open(
		&self,
		_ctx: Context,
		inode: u64,
		_kill_priv: bool,
		_flags: u32,
	) -> Result<(Option<u64>, OpenOptions)> {
		let (handle, _backing_fd) = self.0.open_sync(inode)?;
		Ok((Some(handle), OpenOptions::empty()))
	}

	fn opendir(
		&self,
		_ctx: Context,
		inode: u64,
		_flags: u32,
	) -> Result<(Option<u64>, OpenOptions)> {
		let handle = self.0.opendir_sync(inode)?;
		Ok((Some(handle), OpenOptions::empty()))
	}

	fn read<W: ZeroCopyWriter>(
		&self,
		_ctx: Context,
		_inode: u64,
		handle: u64,
		mut w: W,
		size: u32,
		offset: u64,
		_lock_owner: Option<u64>,
		_flags: u32,
	) -> Result<usize> {
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
		_ctx: Context,
		_inode: u64,
		_flags: u32,
		handle: u64,
		_flush: bool,
		_flock_release: bool,
		_lock_owner: Option<u64>,
	) -> Result<()> {
		self.0.close_sync(handle);
		Ok(())
	}

	fn releasedir(&self, _ctx: Context, _inode: u64, _flags: u32, handle: u64) -> Result<()> {
		self.0.close_sync(handle);
		Ok(())
	}

	fn readdir(
		&self,
		_ctx: Context,
		_inode: u64,
		handle: u64,
		_size: u32,
		offset: u64,
	) -> Result<DirIter> {
		let entries = self.0.readdir_sync(handle)?;
		let items = entries
			.into_iter()
			.map(|(name, id, typ)| {
				let name = CString::new(name)
					.map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
				Ok::<_, io::Error>(Item {
					ino: id,
					type_: dir_entry_type(typ),
					name,
				})
			})
			.collect::<Result<Vec<_>>>()?;
		let cursor = usize::try_from(offset).unwrap_or(0).min(items.len());
		Ok(DirIter {
			entries: items,
			cursor,
		})
	}

	fn getxattr(
		&self,
		_ctx: Context,
		inode: u64,
		name: &CStr,
		size: u32,
	) -> Result<GetxattrReply> {
		let name = name
			.to_str()
			.map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
		let value = self
			.0
			.getxattr_sync(inode, name)?
			.ok_or_else(|| io::Error::from_raw_os_error(libc::ENODATA))?;
		if size == 0 {
			let count = u32::try_from(value.len()).unwrap_or(u32::MAX);
			return Ok(GetxattrReply::Count(count));
		}
		if value.len() > size as usize {
			return Err(io::Error::from_raw_os_error(libc::ERANGE));
		}
		Ok(GetxattrReply::Value(value.to_vec()))
	}

	fn listxattr(&self, _ctx: Context, inode: u64, size: u32) -> Result<ListxattrReply> {
		let names = self.0.listxattrs_sync(inode)?;
		let mut buf = Vec::new();
		for name in &names {
			buf.extend_from_slice(name.as_bytes());
			buf.push(0);
		}
		if size == 0 {
			let count = u32::try_from(buf.len()).unwrap_or(u32::MAX);
			return Ok(ListxattrReply::Count(count));
		}
		if buf.len() > size as usize {
			return Err(io::Error::from_raw_os_error(libc::ERANGE));
		}
		Ok(ListxattrReply::Names(buf))
	}

	fn access(&self, _ctx: Context, _inode: u64, _mask: u32) -> Result<()> {
		Ok(())
	}
}

impl<P> SerializableFileSystem for Adapter<P> where P: Provider + Send + Sync {}

impl DirectoryIterator for DirIter {
	fn next(&mut self) -> Option<DirEntry<'_>> {
		let entry = self.entries.get(self.cursor)?;
		self.cursor += 1;
		Some(DirEntry {
			ino: entry.ino as libc::ino64_t,
			offset: self.cursor as u64,
			type_: entry.type_,
			name: entry.name.as_c_str(),
		})
	}
}

fn attr_from_attrs(inode: u64, attrs: Attrs) -> fuse::Attr {
	let (size, mode) = match attrs.typ {
		FileType::Directory => (0, S_IFDIR | 0o555),
		FileType::File { executable, size } => (
			size,
			S_IFREG | 0o444 | if executable { 0o111 } else { 0 },
		),
		FileType::Symlink => (0, S_IFLNK | 0o444),
	};
	fuse::Attr {
		ino: inode,
		size,
		blocks: 0,
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

fn dir_entry_type(typ: DirEntryType) -> u32 {
	match typ {
		DirEntryType::Directory => u32::from(libc::DT_DIR),
		DirEntryType::File => u32::from(libc::DT_REG),
		DirEntryType::Symlink => u32::from(libc::DT_LNK),
	}
}
