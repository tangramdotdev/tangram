use {foundationdb_tuple as fdbt, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	Identity(tg::process::Id),
	Version {
		partition: u64,
		process: tg::process::Id,
		version: fdbt::Versionstamp,
	},
}
