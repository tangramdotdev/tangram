use {foundationdb_tuple as fdbt, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	UpdateVersion {
		partition: u64,
		version: fdbt::Versionstamp,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
}
