use {
	std::{path::PathBuf, process::ExitCode},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Arg {
	pub artifacts_path: PathBuf,
	pub cpu: Option<u64>,
	pub hostname: Option<String>,
	pub memory: Option<u64>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: bool,
	pub path: PathBuf,
	pub rootfs_path: PathBuf,
	pub tangram_path: PathBuf,
	pub url: tangram_uri::Uri,
	pub user: Option<String>,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	let _ = arg;
	Err(tg::error!("vm isolation is not yet implemented"))
}
