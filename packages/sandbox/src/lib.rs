use {
	bytes::Bytes,
	std::{ffi::OsString, path::PathBuf},
};

pub mod client;
pub mod server;

mod common;
#[cfg(target_os = "macos")]
pub mod darwin;

#[cfg(target_os = "macos")]
pub mod darwin2;

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "linux")]
pub mod linux2;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
	pub stdin: Option<i32>,
	pub stdout: Option<i32>,
	pub stderr: Option<i32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Mount {
	pub source: Option<PathBuf>,
	pub target: Option<PathBuf>,
	pub fstype: Option<OsString>,
	pub flags: libc::c_ulong,
	pub data: Option<Bytes>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Options {
	pub user: Option<String>,
	pub network: bool,
	pub hostname: Option<String>,
	pub chroot: Option<PathBuf>,
	pub mounts: Vec<crate::Mount>,
}
