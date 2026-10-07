use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_usage_started_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
	) -> tg::Result<ControlFlow<Option<i64>, fdb::FdbError>> {
		let key = Self::pack(subspace, &Key::Usage(crate::usage::Key::Started));
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result)
			.map(|bytes| tangram_index::usage::deserialize_timestamp(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(value))
	}

	pub(crate) async fn try_get_usage_unavailable_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		kind: tangram_index::usage::PeriodKind,
		partition: u64,
	) -> tg::Result<ControlFlow<Option<i64>, fdb::FdbError>> {
		let key = Key::Usage(crate::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result)
			.map(|bytes| tangram_index::usage::deserialize_timestamp(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(value))
	}

	pub(crate) fn mark_usage_unavailable_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		kind: tangram_index::usage::PeriodKind,
		partition: u64,
		through: i64,
	) {
		let key = Key::Usage(crate::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let value = tangram_index::usage::serialize_timestamp(through);
		txn.atomic_op(&key, &value, fdb::options::MutationType::Max);
	}
}
