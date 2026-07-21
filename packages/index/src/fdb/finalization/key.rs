use {foundationdb_tuple as fdbt, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	Process(tg::process::Id),
	ProcessVersion {
		id: tg::process::Id,
		partition: u64,
		version: fdbt::Versionstamp,
	},
	Sandbox(tg::sandbox::Id),
	SandboxVersion {
		id: tg::sandbox::Id,
		partition: u64,
		version: fdbt::Versionstamp,
	},
}
