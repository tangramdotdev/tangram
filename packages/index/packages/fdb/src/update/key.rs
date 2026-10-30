use {foundationdb_tuple as fdbt, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Key {
	/// A version-ordered candidate for regular cleaning after older updates have drained.
	Clean {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
		partition: u64,
		version: fdbt::Versionstamp,
	},
	/// The version last used to enqueue this item's parent updates.
	/// An older unchanged update must enqueue the parents again using its older version.
	/// Regular cleaning removes this key once the queue has advanced beyond its version.
	PropagatedVersion {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
	},
	/// A parent that is waiting on a child, which enqueues a storage and metadata update on the parent when the child's update makes progress. The value is the version of the latest propagation to the parent, or empty if there has been none.
	Propagation {
		child: tg::Either<tg::object::Id, tg::process::Id>,
		parent: tg::Either<tg::object::Id, tg::process::Id>,
	},
	/// A complete child whose propagations can be deleted once the storage and metadata queue has advanced beyond the version.
	PropagationClean {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		partition: u64,
		version: fdbt::Versionstamp,
	},
	/// A storage and metadata update queued in version order within a partition, which is written blindly and carries its source in the key.
	StorageAndMetadataUpdate {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		partition: u64,
		source: super::Source,
		version: fdbt::Versionstamp,
	},
	/// A completed traversal can be replayed if an older queue marker arrives later.
	UsageUpdatePropagatedVersion {
		account: tangram_index::usage::Account,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	/// The oldest put version for an account association is independent of its touch timestamp.
	UsageUpdatePutVersion {
		account: tangram_index::usage::Account,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
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
	Permission(tg::authorization::Subject),
	Usage(UsageKind),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UsageKind {
	Clean(tangram_index::usage::Account),
	CleanAll,
	Propagate {
		account: tangram_index::usage::Account,
		touched_at: i64,
	},
	Put {
		account: tangram_index::usage::Account,
		permissions: tg::authorization::permission::Set,
		touched_at: i64,
	},
}
