use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn start_usage(&self, at: jiff::Timestamp) -> tg::Result<()> {
		let subspace = self.subspace.clone();
		crate::fdb::run(&self.database, |txn| {
			let subspace = subspace.clone();
			async move { Self::start_usage_with_transaction(&txn, &subspace, at).await }
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to start usage tracking"))?;

		Ok(())
	}

	async fn start_usage_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		at: jiff::Timestamp,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let result = Self::usage_started_with_transaction(txn, subspace).await;
		let started = crate::fdb::propagate!(result);
		if !started {
			let key = Self::pack(subspace, &Key::Usage(crate::fdb::usage::Key::Started));
			let value = crate::usage::serialize_timestamp(at.as_second());
			txn.set(&key, &value);
		}

		Ok(ControlFlow::Break(()))
	}

	async fn usage_started_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let key = Self::pack(subspace, &Key::Usage(crate::fdb::usage::Key::Started));
		let result = txn.get(&key, false).await;
		let value = crate::fdb::retry!(result);
		let started = value.is_some();

		Ok(ControlFlow::Break(started))
	}

	pub(in crate::fdb) async fn try_get_usage_started_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
	) -> tg::Result<ControlFlow<Option<i64>, fdb::FdbError>> {
		let key = Self::pack(subspace, &Key::Usage(crate::fdb::usage::Key::Started));
		let result = txn.get(&key, false).await;
		let value = crate::fdb::retry!(result)
			.map(|bytes| crate::usage::deserialize_timestamp(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(value))
	}

	pub(in crate::fdb) async fn try_get_usage_unavailable_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		kind: crate::usage::PeriodKind,
		partition: u64,
	) -> tg::Result<ControlFlow<Option<i64>, fdb::FdbError>> {
		let key = Key::Usage(crate::fdb::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let value = crate::fdb::retry!(result)
			.map(|bytes| crate::usage::deserialize_timestamp(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(value))
	}

	pub(in crate::fdb) fn mark_usage_unavailable_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		kind: crate::usage::PeriodKind,
		partition: u64,
		through: i64,
	) {
		let key = Key::Usage(crate::fdb::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let value = crate::usage::serialize_timestamp(through);
		txn.atomic_op(&key, &value, fdb::options::MutationType::Max);
	}
}
