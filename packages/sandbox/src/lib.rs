use {
	bytes::Bytes,
	std::{ffi::OsString, path::PathBuf},
};

mod common;
#[cfg(target_os = "macos")]
pub mod darwin;
#[cfg(target_os = "linux")]
pub mod linux;

#[derive(Debug, Clone)]
pub struct Command {
	pub chroot: Option<PathBuf>,
	pub cwd: Option<PathBuf>,
	pub env: Vec<(String, String)>,
	pub executable: PathBuf,
	pub hostname: Option<String>,
	pub mounts: Vec<Mount>,
	pub network: bool,
	pub trailing: Vec<String>,
	pub user: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Mount {
	pub source: Option<PathBuf>,
	pub target: Option<PathBuf>,
	pub fstype: Option<OsString>,
	pub flags: libc::c_ulong,
	pub data: Option<Bytes>,
}
