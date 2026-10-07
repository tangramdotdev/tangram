use {
	crate::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn get_usage_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &tangram_index::usage::Account,
		period: tangram_index::usage::Period,
		now: jiff::Timestamp,
		partition_total: u64,
	) -> tg::Result<tangram_index::usage::Aggregate> {
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
			return Ok(tangram_index::usage::Aggregate::default());
		}

		let mut aggregate = tangram_index::usage::PartitionAggregate::default();
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
		account: &tangram_index::usage::Account,
		partition: u64,
		period: tangram_index::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<tangram_index::usage::PartitionAggregate> {
		if period.start() > now {
			return Ok(tangram_index::usage::PartitionAggregate::default());
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
			tangram_index::usage::Period::Hour(_) => Self::aggregate_usage_hour_with_transaction(
				db,
				subspace,
				transaction,
				account,
				period.start().as_second(),
				partition,
				period.end() <= now,
			)?,
			tangram_index::usage::Period::Day(_)
			| tangram_index::usage::Period::Month(_)
			| tangram_index::usage::Period::Week(_) => {
				let mut aggregate = tangram_index::usage::PartitionAggregate::default();
				for child in tangram_index::usage::children(period)? {
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
