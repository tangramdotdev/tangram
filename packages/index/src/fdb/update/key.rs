use {foundationdb_tuple as fdbt, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
	},
	UpdateBarrier {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
		partition: u64,
		version: fdbt::Versionstamp,
	},
	UpdateVersion {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
		partition: u64,
		version: fdbt::Versionstamp,
	},
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Kind {
	Item,
	Grants(tg::grant::Principal),
}
