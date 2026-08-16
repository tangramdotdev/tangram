use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
};

impl Index {
	pub(crate) fn add_usage_delta(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		at: i64,
		kind: crate::usage::DeltaKind,
		delta: i64,
		partition: u64,
	) {
		let hour = at.div_euclid(60 * 60) * 60 * 60;
		let key = Key::Usage(crate::fdb::usage::Key::Delta {
			account: account.clone(),
			hour,
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.atomic_op(&key, &delta.to_le_bytes(), fdb::options::MutationType::Add);
		let key = Key::Usage(crate::fdb::usage::Key::Compaction {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);
	}
}
