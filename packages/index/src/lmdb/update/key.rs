use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
	},
	UpdateVersion {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
		version: u64,
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
	Add {
		account: crate::usage::Account,
		touched_at: i64,
	},
	Clean(crate::usage::Account),
	CleanAll,
	Propagate {
		account: crate::usage::Account,
		touched_at: i64,
	},
}
