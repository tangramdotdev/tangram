use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn start_usage(&self, at: jiff::Timestamp) -> tg::Result<()> {
		let db = self.db;
		let env = self.env.clone();
		let subspace = self.subspace.clone();
		tokio::task::spawn_blocking(move || {
			let mut transaction = env
				.write_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a write transaction"))?;
			Self::start_usage_with_transaction(db, &subspace, &mut transaction, at)?;
			transaction
				.commit()
				.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

			Ok::<_, tg::Error>(())
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;

		Ok(())
	}

	fn start_usage_with_transaction(
		db: Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		at: jiff::Timestamp,
	) -> tg::Result<()> {
		let key = Self::pack(subspace, &Key::Usage(crate::usage::Key::Started));
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the usage start time"))?;
		if value.is_none() {
			let value = tangram_index::usage::serialize_timestamp(at.as_second());
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the usage start time"))?;
		}

		Ok(())
	}

	pub(crate) fn try_get_usage_started_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
	) -> tg::Result<Option<i64>> {
		let key = Self::pack(subspace, &Key::Usage(crate::usage::Key::Started));
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the usage start time"))?
			.map(tangram_index::usage::deserialize_timestamp)
			.transpose()?;

		Ok(value)
	}

	pub(crate) fn try_get_usage_unavailable_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		kind: tangram_index::usage::PeriodKind,
		partition: u64,
	) -> tg::Result<Option<i64>> {
		let key = Key::Usage(crate::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the unavailable usage cutoff"))?
			.map(tangram_index::usage::deserialize_timestamp)
			.transpose()?;

		Ok(value)
	}

	pub(crate) fn mark_usage_unavailable_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		kind: tangram_index::usage::PeriodKind,
		partition: u64,
		through: i64,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let previous = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the unavailable usage cutoff"))?
			.map(tangram_index::usage::deserialize_timestamp)
			.transpose()?;
		let through = previous.map_or(through, |previous| previous.max(through));
		let value = tangram_index::usage::serialize_timestamp(through);
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the unavailable usage cutoff"))?;

		Ok(())
	}
}
