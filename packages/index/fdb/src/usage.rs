use {
	crate::{Index, Key as IndexKey, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
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
		let subspace = self.subspace.clone();
		crate::run(&self.database, |txn| {
			let subspace = subspace.clone();
			async move { Self::start_usage_with_transaction(&txn, &subspace, at).await }
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to start usage tracking"))?;

		Ok(())
	}

	async fn start_usage_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		at: jiff::Timestamp,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let result = Self::usage_started_with_transaction(txn, subspace).await;
		let started = crate::propagate!(result);
		if !started {
			let key = Self::pack(subspace, &IndexKey::Usage(crate::usage::Key::Started));
			let value = tangram_index::usage::serialize_timestamp(at.as_second());
			txn.set(&key, &value);
		}

		Ok(ControlFlow::Break(()))
	}

	async fn usage_started_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let key = Self::pack(subspace, &IndexKey::Usage(crate::usage::Key::Started));
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);
		let started = value.is_some();

		Ok(ControlFlow::Break(started))
	}

	#[must_use]
	pub fn usage_partition_total(&self) -> u64 {
		self.partition_totals.usage
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
