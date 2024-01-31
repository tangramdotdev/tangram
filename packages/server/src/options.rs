use std::path::PathBuf;
use tangram_client as tg;

pub struct Options {
	pub address: tg::Address,
	pub build: Build,
	pub path: PathBuf,
	pub remote: Option<Remote>,
	pub version: String,
	pub vfs: Vfs,
}

pub struct Build {
	pub permits: usize,
}

pub struct Remote {
	pub build: RemoteBuild,
	pub tg: Box<dyn tg::Handle>,
}

pub struct RemoteBuild {
	pub enable: bool,
	pub hosts: Vec<tg::System>,
}

pub struct Vfs {
	pub enable: bool,
}
