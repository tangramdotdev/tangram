use tangram_client::prelude::*;

#[derive(Debug)]
pub enum Key {
	Process(tg::process::Id),
	ProcessVersion { id: tg::process::Id, version: u64 },
	Sandbox(tg::sandbox::Id),
	SandboxVersion { id: tg::sandbox::Id, version: u64 },
}
