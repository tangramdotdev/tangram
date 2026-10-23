use {
	crate::{Db, Index, Key as IndexKey, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

mod aggregate;
mod compute;
mod expire;
mod get;
mod key;
mod put;
mod start;
pub(super) mod storage;

pub(super) use key::Key;

impl Index {
	pub async fn expire_usage(
		&self,
		arg: tangram_index::usage::expire::Arg,
	) -> tg::Result<tangram_index::usage::expire::Output> {
		let response = self.send_write_request(Request::ExpireUsage(arg)).await?;
		let Response::ExpireUsageOutput(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

	pub async fn aggregate_usage(
		&self,
		arg: tangram_index::usage::aggregate::Arg,
	) -> tg::Result<tangram_index::usage::aggregate::Output> {
		let response = self
			.send_write_request(Request::AggregateUsage(arg))
			.await?;
		let Response::AggregateUsageOutput(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

	pub async fn get_usage(
		&self,
		account: &tangram_index::usage::Account,
		period: tangram_index::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<tangram_index::usage::Aggregate> {
		let request = Request::GetUsage {
			account: account.clone(),
			now,
			period,
		};
		let response = self.send_write_request(request).await?;
		let Response::Usage(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

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
		let key = Self::pack(subspace, &IndexKey::Usage(crate::usage::Key::Started));
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

	#[must_use]
	pub fn usage_partition_total(&self) -> u64 {
		self.usage_partition_total
	}
}

impl tangram_index::usage::Index for Index {
	async fn expire_usage(
		&self,
		arg: tangram_index::usage::expire::Arg,
	) -> tg::Result<tangram_index::usage::expire::Output> {
		self.expire_usage(arg).await
	}

	async fn aggregate_usage(
		&self,
		arg: tangram_index::usage::aggregate::Arg,
	) -> tg::Result<tangram_index::usage::aggregate::Output> {
		self.aggregate_usage(arg).await
	}

	async fn get_usage(
		&self,
		account: &tangram_index::usage::Account,
		period: tangram_index::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<tangram_index::usage::Aggregate> {
		self.get_usage(account, period, now).await
	}

	async fn start_usage(&self, at: jiff::Timestamp) -> tg::Result<()> {
		self.start_usage(at).await
	}

	fn usage_partition_total(&self) -> u64 {
		self.usage_partition_total()
	}
}
