use tangram_client::prelude::*;

#[derive(Debug)]
pub enum Key {
	Identity(tg::process::Id),
	Version {
		process: tg::process::Id,
		version: u64,
	},
}
