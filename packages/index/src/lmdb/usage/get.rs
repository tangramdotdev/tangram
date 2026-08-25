use {
	crate::lmdb::{Db, Index, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_usage(
		&self,
		account: &crate::usage::Account,
		period: crate::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<crate::usage::Aggregate> {
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

	pub(crate) fn get_usage_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		period: crate::usage::Period,
		now: jiff::Timestamp,
		partition_total: u64,
	) -> tg::Result<crate::usage::Aggregate> {
		let started = Self::try_get_usage_started_with_transaction(db, subspace, transaction)?
			.ok_or_else(|| tg::error!("usage tracking has not started"))?;
		if period.start().as_second() < started && period.end() <= now {
			return Err(tg::error!("usage is unavailable for the requested period"));
		}
		for partition in 0..partition_total {
			let cutoff = Self::try_get_usage_unavailable_with_transaction(
				db,
				subspace,
				transaction,
				account,
				period.kind(),
				partition,
			)?;
			if cutoff.is_some_and(|cutoff| period.end().as_second() <= cutoff) {
				return Err(tg::error!("usage is unavailable for the requested period"));
			}
		}
		if period.start() > now {
			return Ok(crate::usage::Aggregate::default());
		}

		let mut aggregate = crate::usage::PartitionAggregate::default();
		let current_hour = now.as_second().div_euclid(60 * 60) * 60 * 60;
		let end_hour = period.end().as_second().min(current_hour);
		for partition in 0..partition_total {
			Self::aggregate_usage_for_account_with_transaction(
				db,
				subspace,
				transaction,
				account,
				partition,
				end_hour,
				None,
			)?;
			let value = Self::aggregate_usage_period_with_transaction(
				db,
				subspace,
				transaction,
				account,
				partition,
				period,
				now,
			)?;
			aggregate.checked_add(value)?;
		}

		aggregate.try_into_aggregate()
	}

	fn aggregate_usage_period_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<crate::usage::PartitionAggregate> {
		if period.start() > now {
			return Ok(crate::usage::PartitionAggregate::default());
		}
		if period.end() <= now {
			let aggregate = Self::try_get_usage_aggregate_with_transaction(
				db,
				subspace,
				transaction,
				account,
				partition,
				period,
			)?;

			return Ok(aggregate.unwrap_or_default());
		}

		let aggregate = match period {
			crate::usage::Period::Hour(_) => Self::aggregate_usage_hour_with_transaction(
				db,
				subspace,
				transaction,
				account,
				period.start().as_second(),
				partition,
				period.end() <= now,
			)?,
			crate::usage::Period::Day(_)
			| crate::usage::Period::Month(_)
			| crate::usage::Period::Week(_) => {
				let mut aggregate = crate::usage::PartitionAggregate::default();
				for child in crate::usage::children(period)? {
					if child.start() > now {
						break;
					}
					let child = Self::aggregate_usage_period_with_transaction(
						db,
						subspace,
						transaction,
						account,
						partition,
						child,
						now,
					)?;
					aggregate.checked_add(child)?;
				}
				if period.end() <= now {
					Self::put_usage_aggregate_with_transaction(
						db,
						subspace,
						transaction,
						account,
						partition,
						period,
						aggregate,
					)?;
				}
				aggregate
			},
		};

		Ok(aggregate)
	}
}
