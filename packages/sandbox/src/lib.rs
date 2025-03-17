use std::{
	ffi::{OsStr, OsString},
	os::unix::ffi::OsStrExt,
	path::PathBuf,
};
use tangram_either::Either;
use tokio::io::{AsyncRead, AsyncWrite};

mod common;
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;
mod pty;

pub enum MountKind {
	Bind,
}

pub struct Overlay {
	pub lowerdirs: Vec<PathBuf>,
	pub upperdir: PathBuf,
	pub workdir: PathBuf,
	pub merged: PathBuf,
	pub readonly: bool,
}

pub struct BindMount {
	pub source: PathBuf,
	pub target: PathBuf,
	pub readonly: bool,
}

#[allow(dead_code)]
pub struct Command {
	args: Vec<OsString>,
	chroot: Option<PathBuf>,
	cwd: PathBuf,
	envs: Vec<(OsString, OsString)>,
	executable: PathBuf,
	gid: u32,
	mounts: Vec<Mount>,
	network: bool,
	paths: Vec<Path>,
	sandbox: bool,
	stdin: Stdio,
	stdout: Stdio,
	stderr: Stdio,
	uid: u32,
}

pub struct Child {
	#[cfg(target_os = "macos")]
	pid: libc::pid_t,

	#[cfg(target_os = "linux")]
	guest_pid: libc::pid_t,

	#[cfg(target_os = "linux")]
	root_pid: libc::pid_t,

	#[cfg(target_os = "linux")]
	socket: tokio::net::UnixStream,

	pub stdin: Option<Stdin>,
	pub stdout: Option<Stdout>,
	pub stderr: Option<Stderr>,
}

pub struct Path {
	pub path: PathBuf,
	pub readonly: bool,
}

#[derive(Clone, Debug)]
pub struct Mount {
	pub source: PathBuf,
	pub target: PathBuf,
	pub fstype: Option<OsString>,
	pub flags: libc::c_ulong,
	pub data: Option<Vec<u8>>,
	pub readonly: bool,
}

pub struct Stdin {
	inner: Either<pty::Writer, tokio::net::UnixStream>,
}

pub struct Stdout {
	inner: Either<pty::Reader, tokio::net::UnixStream>,
}

pub struct Stderr {
	inner: Either<pty::Reader, tokio::net::UnixStream>,
}

#[derive(Copy, Clone, Debug)]
pub enum Stdio {
	Inherit,
	Piped,
	Null,
	Tty(Tty),
}

#[derive(Copy, Clone, Debug)]
pub struct Tty {
	pub rows: u16,
	pub cols: u16,
	pub x: u16,
	pub y: u16,
}

#[derive(Debug)]
pub enum ExitStatus {
	Code(i32),
	Signal(i32),
}

impl Command {
	pub fn new(executable: impl AsRef<std::path::Path>) -> Self {
		Self {
			args: Vec::new(),
			chroot: None,
			cwd: std::env::current_dir().unwrap(),
			envs: Vec::new(),
			executable: executable.as_ref().to_owned(),
			gid: unsafe { libc::getgid() },
			mounts: Vec::new(),
			network: true,
			paths: Vec::new(),
			sandbox: false,
			stdin: Stdio::Inherit,
			stdout: Stdio::Inherit,
			stderr: Stdio::Inherit,
			uid: unsafe { libc::getuid() },
		}
	}

	pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
		self.args(std::iter::once(arg))
	}

	pub fn args(&mut self, args: impl IntoIterator<Item = impl AsRef<OsStr>>) -> &mut Self {
		self.args
			.extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
		self
	}

	pub fn chroot(&mut self, p: impl AsRef<std::path::Path>) -> &mut Self {
		self.chroot.replace(p.as_ref().to_owned());
		self
	}

	pub fn cwd(&mut self, cwd: impl AsRef<std::path::Path>) -> &mut Self {
		self.cwd = cwd.as_ref().to_owned();
		self
	}

	pub fn env(&mut self, k: impl AsRef<OsStr>, v: impl AsRef<OsStr>) -> &mut Self {
		self.envs(std::iter::once((k, v)))
	}

	pub fn envs(
		&mut self,
		envs: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
	) -> &mut Self {
		self.envs.extend(
			envs.into_iter()
				.map(|(k, v)| (k.as_ref().to_owned(), v.as_ref().to_owned())),
		);
		self
	}

	pub fn gid(&mut self, gid: libc::gid_t) -> &mut Self {
		self.gid = gid;
		self
	}

	pub fn uid(&mut self, uid: libc::uid_t) -> &mut Self {
		self.uid = uid;
		self
	}

	pub fn mount(&mut self, mount: impl Into<Mount>) -> &mut Self {
		self.mounts(std::iter::once(mount))
	}

	pub fn mounts(&mut self, mounts: impl IntoIterator<Item = impl Into<Mount>>) -> &mut Self {
		self.mounts.extend(mounts.into_iter().map(Into::into));
		self
	}

	pub fn network(&mut self, enable: bool) -> &mut Self {
		self.network = enable;
		self
	}

	pub fn sandbox(&mut self, enable: bool) -> &mut Self {
		self.sandbox = enable;
		self
	}

	pub fn stdin(&mut self, stdio: Stdio) -> &mut Self {
		self.stdin = stdio;
		self
	}

	pub fn stdout(&mut self, stdio: Stdio) -> &mut Self {
		self.stdout = stdio;
		self
	}

	pub fn stderr(&mut self, stdio: Stdio) -> &mut Self {
		self.stderr = stdio;
		self
	}

	pub fn spawn(&self) -> impl Future<Output = std::io::Result<Child>> {
		#[cfg(target_os = "linux")]
		{
			linux::spawn(self)
		}
		#[cfg(target_os = "macos")]
		{
			darwin::spawn(self)
		}
	}
}

impl Child {
	pub fn pid(&self) -> libc::pid_t {
		#[cfg(target_os = "linux")]
		{
			self.guest_pid
		}
		#[cfg(target_os = "macos")]
		{
			self.pid
		}
	}

	pub fn wait(&mut self) -> impl Future<Output = std::io::Result<ExitStatus>> {
		#[cfg(target_os = "linux")]
		{
			linux::wait(self)
		}
		#[cfg(target_os = "macos")]
		{
			darwin::wait(self)
		}
	}
}

impl Stdin {
	pub async fn change_window_size(&self, tty: Tty) -> std::io::Result<()> {
		let Either::Left(pty) = &self.inner else {
			return Err(std::io::Error::other("not a pty"));
		};
		pty.change_window_size(tty).await
	}
}

impl AsyncWrite for Stdin {
	fn is_write_vectored(&self) -> bool {
		match &self.inner {
			Either::Left(io) => io.is_write_vectored(),
			Either::Right(io) => io.is_write_vectored(),
		}
	}

	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(io) => std::pin::pin!(io).poll_flush(cx),
			Either::Right(io) => std::pin::pin!(io).poll_flush(cx),
		}
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(io) => std::pin::pin!(io).poll_shutdown(cx),
			Either::Right(io) => std::pin::pin!(io).poll_shutdown(cx),
		}
	}

	fn poll_write(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(io) => std::pin::pin!(io).poll_write(cx, buf),
			Either::Right(io) => std::pin::pin!(io).poll_write(cx, buf),
		}
	}

	fn poll_write_vectored(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		bufs: &[std::io::IoSlice<'_>],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(io) => std::pin::pin!(io).poll_write_vectored(cx, bufs),
			Either::Right(io) => std::pin::pin!(io).poll_write_vectored(cx, bufs),
		}
	}
}

impl AsyncRead for Stdout {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match &mut self.get_mut().inner {
			Either::Left(io) => std::pin::pin!(io).poll_read(cx, buf),
			Either::Right(io) => std::pin::pin!(io).poll_read(cx, buf),
		}
	}
}

impl AsyncRead for Stderr {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match &mut self.get_mut().inner {
			Either::Left(io) => std::pin::pin!(io).poll_read(cx, buf),
			Either::Right(io) => std::pin::pin!(io).poll_read(cx, buf),
		}
	}
}

impl Drop for Child {
	fn drop(&mut self) {
		#[cfg(target_os = "linux")]
		let options = libc::__WALL;

		#[cfg(not(target_os = "linux"))]
		let options = libc::WEXITED;

		let pid = self.pid();
		tokio::task::spawn_blocking(move || {
			unsafe { libc::kill(pid, libc::SIGKILL) };
			let mut status = 0;
			unsafe {
				libc::waitpid(pid, std::ptr::addr_of_mut!(status), options);
			}
		});
	}
}

impl<S, T> From<(S, T)> for Mount
where
	S: AsRef<std::path::Path>,
	T: AsRef<std::path::Path>,
{
	fn from(value: (S, T)) -> Self {
		let (source, target) = value;
		let source = source.as_ref().to_owned();
		let target = target.as_ref().to_owned();
		Mount::from(BindMount {
			source,
			target,
			readonly: true,
		})
	}
}

impl From<Overlay> for Mount {
	fn from(value: Overlay) -> Self {
		fn escape(out: &mut Vec<u8>, path: &[u8]) {
			for byte in path.iter().copied() {
				if byte == 0 {
					break;
				}
				if byte == b':' {
					out.push(b'\\');
				}
				out.push(byte);
			}
		}

		// Create the mount options.
		let mut data = vec![];

		// Add the lower directories.
		data.extend_from_slice(b"userxattr,lowerdir=");
		for (n, dir) in value.lowerdirs.iter().enumerate() {
			escape(&mut data, dir.as_os_str().as_bytes());
			if n != value.lowerdirs.len() - 1 {
				data.push(b':');
			}
		}

		// Add the upper directory.
		data.extend_from_slice(b",upperdir=");
		data.extend_from_slice(value.upperdir.as_os_str().as_bytes());

		// Add the working directory.
		data.extend_from_slice(b",workdir=");
		data.extend_from_slice(value.workdir.as_os_str().as_bytes());
		data.push(0);

		// Create the mount.
		Mount {
			source: "overlay".into(),
			target: value.merged,
			fstype: Some("overlay".into()),
			flags: 0,
			data: Some(data),
			readonly: false,
		}
	}
}

impl From<BindMount> for Mount {
	fn from(value: BindMount) -> Self {
		Mount {
			source: value.source,
			target: value.target,
			fstype: None,
			#[cfg(target_os = "linux")]
			flags: libc::MS_BIND | libc::MS_REC,
			#[cfg(target_os = "macos")]
			flags: 0,
			data: None,
			readonly: value.readonly,
		}
	}
}
