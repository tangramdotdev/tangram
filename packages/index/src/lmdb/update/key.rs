use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	/// A version-ordered candidate for regular cleaning after older updates have drained.
	Clean {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
		version: u64,
	},
	/// The version last used to enqueue this item's parent updates.
	/// An older unchanged update must enqueue the parents again using its older version.
	/// Regular cleaning removes this key once the queue has advanced beyond its version.
	PropagatedVersion {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
	},
	/// The oldest put version for an account association is independent of its touch timestamp.
	StorageUpdatePutVersion {
		account: crate::usage::Account,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
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
	Grant(tg::authorization::Subject),
	Node,
	Storage(StorageKind),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageKind {
	Clean(crate::usage::Account),
	CleanAll,
	Propagate {
		account: crate::usage::Account,
		touched_at: i64,
	},
	Put {
		account: crate::usage::Account,
		touched_at: i64,
	},
}
