use {foundationdb_tuple as fdbt, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
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
	Grant(tg::grant::Principal),
	Node,
	Storage(StorageKind),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageKind {
	Add(crate::usage::Account),
	Clean(crate::usage::Account),
	CleanAll,
	Propagate(crate::usage::Account),
}
