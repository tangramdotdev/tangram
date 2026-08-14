use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn start_usage(&self, at: jiff::Timestamp) -> tg::Result<()> {
		let subspace = self.subspace.clone();
		self.database
			.run(|txn, _| {
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
	) -> crate::fdb::Result<()> {
		let key = Self::pack(subspace, &Key::Usage(crate::fdb::usage::Key::Started));
		let value = txn.get(&key, false).await?;
		if value.is_none() {
			let value = crate::usage::serialize_timestamp(at.as_second());
			txn.set(&key, &value);
		}

		Ok(())
	}

	pub(in crate::fdb) async fn try_get_usage_started_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
	) -> crate::fdb::Result<Option<i64>> {
		let key = Self::pack(subspace, &Key::Usage(crate::fdb::usage::Key::Started));
		let value = txn
			.get(&key, false)
			.await?
			.map(|bytes| crate::usage::deserialize_timestamp(&bytes))
			.transpose()
			.map_err(crate::fdb::custom_error)?;

		Ok(value)
	}

	pub(in crate::fdb) async fn try_get_usage_unavailable_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		kind: crate::usage::PeriodKind,
		partition: u64,
	) -> crate::fdb::Result<Option<i64>> {
		let key = Key::Usage(crate::fdb::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let value = txn
			.get(&key, false)
			.await?
			.map(|bytes| crate::usage::deserialize_timestamp(&bytes))
			.transpose()
			.map_err(crate::fdb::custom_error)?;

		Ok(value)
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
