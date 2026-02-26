use {
	bytes::Bytes,
	std::{ffi::OsString, path::PathBuf},
};

pub mod client;
pub mod server;

mod common;
mod daemon;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Command {
	pub cwd: Option<PathBuf>,
	pub env: Vec<(String, String)>,
	pub executable: PathBuf,
	pub trailing: Vec<String>,
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
