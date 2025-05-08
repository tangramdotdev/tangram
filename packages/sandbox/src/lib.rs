use std::{
	ffi::{OsStr, OsString},
	os::unix::ffi::OsStrExt,
	path::PathBuf,
};

mod common;
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;
// mod pty;
mod stdio;
pub use stdio::*;

#[allow(dead_code)]
pub struct Command {
	args: Vec<OsString>,
	chroot: Option<PathBuf>,
	cwd: PathBuf,
	envs: Vec<(OsString, OsString)>,
	executable: PathBuf,
	gid: Option<u32>,
	hostname: Option<OsString>,
	mounts: Vec<Mount>,
	network: bool,
	stdin: Option<Stdio>,
	stdout: Option<Stdio>,
	stderr: Option<Stdio>,
	uid: Option<u32>,
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

	pub stdin: Option<ChildStdin>,
	pub stdout: Option<ChildStdout>,
	pub stderr: Option<ChildStderr>,
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

pub struct Overlay {
	pub lowerdirs: Vec<PathBuf>,
	pub upperdir: PathBuf,
	pub workdir: PathBuf,
	pub merged: PathBuf,
}

pub struct BindMount {
	pub source: PathBuf,
	pub target: PathBuf,
	pub readonly: bool,
}

#[derive(Copy, Clone, Debug)]
pub struct Tty {
	pub rows: u16,
	pub cols: u16,
}

#[derive(Debug)]
pub enum ExitStatus {
	Code(u8),
	Signal(u8),
}

impl Command {
	pub fn new(executable: impl AsRef<std::path::Path>) -> Self {
		Self {
			args: Vec::new(),
			chroot: None,
			cwd: std::env::current_dir().unwrap(),
			envs: Vec::new(),
			executable: executable.as_ref().to_owned(),
			gid: None,
			hostname: None,
			mounts: Vec::new(),
			network: true,
			stdin: Some(Stdio::inherit()),
			stdout: Some(Stdio::inherit()),
			stderr: Some(Stdio::inherit()),
			uid: None,
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

	pub fn chroot(&mut self, path: impl AsRef<std::path::Path>) -> &mut Self {
		self.chroot.replace(path.as_ref().to_owned());
		self
	}

	pub fn cwd(&mut self, cwd: impl AsRef<std::path::Path>) -> &mut Self {
		self.cwd = cwd.as_ref().to_owned();
		self
	}

	pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
		self.envs(std::iter::once((key, value)))
	}

	pub fn envs(
		&mut self,
		envs: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
	) -> &mut Self {
		self.envs.extend(
			envs.into_iter()
				.map(|(key, value)| (key.as_ref().to_owned(), value.as_ref().to_owned())),
		);
		self
	}

	pub fn gid(&mut self, gid: libc::gid_t) -> &mut Self {
		self.gid.replace(gid);
		self
	}

	pub fn uid(&mut self, uid: libc::uid_t) -> &mut Self {
		self.uid.replace(uid);
		self
	}

	pub fn hostname(&mut self, hostname: impl AsRef<OsStr>) -> &mut Self {
		self.hostname.replace(hostname.as_ref().to_owned());
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

	pub fn stdin(&mut self, stdio: impl Into<Stdio>) -> &mut Self {
		self.stdin.replace(stdio.into());
		self
	}

	pub fn stdout(&mut self, stdio: impl Into<Stdio>) -> &mut Self {
		self.stdout.replace(stdio.into());
		self
	}

	pub fn stderr(&mut self, stdio: impl Into<Stdio>) -> &mut Self {
		self.stderr.replace(stdio.into());
		self
	}

	pub fn spawn(&mut self) -> impl Future<Output = std::io::Result<Child>> {
		#[cfg(target_os = "linux")]
		{
			linux::spawn(self)
		}
		#[cfg(target_os = "macos")]
		{
			darwin::spawn(self)
		}
	}

	pub fn user(&mut self, name: impl AsRef<OsStr>) -> std::io::Result<&mut Self> {
		unsafe {
			let passwd = libc::getpwnam(common::cstring(name.as_ref()).as_ptr());
			if passwd.is_null() {
				return Err(std::io::Error::other("getpwname failed"));
			}
			let uid = (*passwd).pw_uid;
			let gid = (*passwd).pw_gid;
			self.uid(uid).gid(gid);
			Ok(self)
		}
	}
}

impl Child {
	#[must_use]
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
	S: Into<PathBuf>,
	T: Into<PathBuf>,
{
	fn from(value: (S, T)) -> Self {
		let (source, target) = value;
		let source = source.into();
		let target = target.into();
		Mount::from(BindMount {
			source,
			target,
			readonly: true,
		})
	}
}

impl<S, T> From<(S, T, bool)> for Mount
where
	S: Into<PathBuf>,
	T: Into<PathBuf>,
{
	fn from(value: (S, T, bool)) -> Self {
		let (source, target, readonly) = value;
		let source = source.into();
		let target = target.into();
		Mount::from(BindMount {
			source,
			target,
			readonly,
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
