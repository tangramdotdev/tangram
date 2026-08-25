use {
	crate::lmdb::{Db, Index, Key, Kind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

#[derive(Default)]
struct Deltas {
	object_count: i128,
	object_size: i128,
	process_count: i128,
	sandbox_count: i128,
	sandbox_cpu: i128,
	sandbox_memory: i128,
}

impl Index {
	pub async fn aggregate_usage(
		&self,
		arg: crate::usage::aggregate::Arg,
	) -> tg::Result<crate::usage::aggregate::Output> {
		let response = self
			.send_write_request(Request::AggregateUsage(arg))
			.await?;
		let Response::AggregateUsageOutput(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

	pub(crate) fn aggregate_usage_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::usage::aggregate::Arg,
	) -> tg::Result<crate::usage::aggregate::Output> {
		let current_hour = arg.now.as_second().div_euclid(60 * 60) * 60 * 60;
		let mut candidates = Vec::new();
		for partition in arg.partition_start..arg.partition_end {
			let prefix = Self::pack(
				subspace,
				&(Kind::UsageAggregation.to_i32().unwrap(), partition),
			);
			let entries = db
				.prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to iterate usage aggregations"))?;
			for entry in entries {
				if candidates.len() == arg.batch_size {
					break;
				}
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read a usage aggregation"))?;
				let Key::Usage(crate::lmdb::usage::Key::Aggregation {
					account,
					hour,
					partition,
				}) = Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				if hour >= current_hour {
					break;
				}
				candidates.push((account, hour, partition));
			}
			if candidates.len() == arg.batch_size {
				break;
			}
		}

		let mut count = 0;
		for (account, _, partition) in &candidates {
			let limit = arg.batch_size - count;
			let value = Self::aggregate_usage_for_account_with_transaction(
				db,
				subspace,
				transaction,
				account,
				*partition,
				current_hour,
				Some(limit),
			)?;
			count += value;
			if count == arg.batch_size {
				break;
			}
		}
		let output = crate::usage::aggregate::Output { count };

		Ok(output)
	}

	pub(in crate::lmdb) fn aggregate_usage_for_account_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		end_hour: i64,
		limit: Option<usize>,
	) -> tg::Result<usize> {
		let mut count = 0;
		loop {
			if limit.is_some_and(|limit| count == limit) {
				break;
			}
			let hour = Self::try_get_usage_aggregation_for_account_with_transaction(
				db,
				subspace,
				transaction,
				account,
				partition,
				end_hour,
			)?;
			let Some(hour) = hour else {
				break;
			};
			Self::aggregate_usage_hour_with_transaction(
				db,
				subspace,
				transaction,
				account,
				hour,
				partition,
				true,
			)?;
			count += 1;
		}

		Ok(count)
	}

	pub(in crate::lmdb) fn aggregate_usage_hour_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
		clear_aggregation: bool,
	) -> tg::Result<crate::usage::PartitionAggregate> {
		let period =
			crate::usage::Period::from_kind_and_start(crate::usage::PeriodKind::Hour, hour)?;
		let old = Self::try_get_usage_aggregate_with_transaction(
			db,
			subspace,
			transaction,
			account,
			partition,
			period,
		)?
		.unwrap_or_default();
		let previous = match hour.checked_sub(60 * 60) {
			Some(start) => {
				let period = crate::usage::Period::from_kind_and_start(
					crate::usage::PeriodKind::Hour,
					start,
				)?;
				Self::try_get_usage_aggregate_with_transaction(
					db,
					subspace,
					transaction,
					account,
					partition,
					period,
				)?
				.unwrap_or_default()
			},
			None => crate::usage::PartitionAggregate::default(),
		};
		let deltas = Self::get_usage_deltas_with_transaction(
			db,
			subspace,
			transaction,
			account,
			hour,
			partition,
		)?;
		let sandbox_cpu = u128::try_from(deltas.sandbox_cpu)
			.map_err(|_| tg::error!("the sandbox CPU usage is out of range"))?;
		let sandbox_memory = u128::try_from(deltas.sandbox_memory)
			.map_err(|_| tg::error!("the sandbox memory usage is out of range"))?;
		let aggregate = crate::usage::PartitionAggregate {
			object_count: apply_delta(previous.object_count, deltas.object_count)?,
			object_size: apply_delta(previous.object_size, deltas.object_size)?,
			process_count: apply_delta(previous.process_count, deltas.process_count)?,
			sandbox_count: deltas.sandbox_count,
			sandbox_cpu,
			sandbox_memory,
		};
		if !clear_aggregation {
			return Ok(aggregate);
		}
		Self::put_usage_aggregate_with_transaction(
			db,
			subspace,
			transaction,
			account,
			partition,
			period,
			aggregate,
		)?;
		Self::clear_usage_aggregation_with_transaction(
			db,
			subspace,
			transaction,
			account,
			hour,
			partition,
		)?;

		let changed = aggregate != old;
		if storage_changed(old, aggregate) {
			let next = hour
				.checked_add(60 * 60)
				.ok_or_else(|| tg::error!("the usage hour overflowed"))?;
			Self::put_usage_aggregation_with_transaction(
				db,
				subspace,
				transaction,
				account,
				next,
				partition,
			)?;
		}
		let day = crate::usage::Period::containing(crate::usage::PeriodKind::Day, period.start());
		let closing_hour = crate::usage::closing_hour(day)?;
		if hour == closing_hour {
			Self::aggregate_usage_day_with_transaction(
				db,
				subspace,
				transaction,
				account,
				partition,
				day,
				hour,
			)?;
		} else if changed {
			Self::put_usage_aggregation_with_transaction(
				db,
				subspace,
				transaction,
				account,
				closing_hour,
				partition,
			)?;
		}

		Ok(aggregate)
	}

	pub(in crate::lmdb) fn aggregate_usage_day_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
		hour: i64,
	) -> tg::Result<crate::usage::PartitionAggregate> {
		let old = Self::try_get_usage_aggregate_with_transaction(
			db,
			subspace,
			transaction,
			account,
			partition,
			period,
		)?
		.unwrap_or_default();
		let aggregate = Self::sum_usage_children_with_transaction(
			db,
			subspace,
			transaction,
			account,
			partition,
			period,
		)?;
		Self::put_usage_aggregate_with_transaction(
			db,
			subspace,
			transaction,
			account,
			partition,
			period,
			aggregate,
		)?;

		for kind in [
			crate::usage::PeriodKind::Week,
			crate::usage::PeriodKind::Month,
		] {
			let parent = crate::usage::Period::containing(kind, period.start());
			let closing_hour = crate::usage::closing_hour(parent)?;
			if hour == closing_hour {
				Self::aggregate_usage_parent_with_transaction(
					db,
					subspace,
					transaction,
					account,
					partition,
					parent,
				)?;
			} else if aggregate != old {
				Self::put_usage_aggregation_with_transaction(
					db,
					subspace,
					transaction,
					account,
					closing_hour,
					partition,
				)?;
			}
		}

		Ok(aggregate)
	}

	pub(in crate::lmdb) fn aggregate_usage_parent_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
	) -> tg::Result<crate::usage::PartitionAggregate> {
		let aggregate = Self::sum_usage_children_with_transaction(
			db,
			subspace,
			transaction,
			account,
			partition,
			period,
		)?;
		Self::put_usage_aggregate_with_transaction(
			db,
			subspace,
			transaction,
			account,
			partition,
			period,
			aggregate,
		)?;

		Ok(aggregate)
	}

	pub(in crate::lmdb) fn try_get_usage_aggregate_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
	) -> tg::Result<Option<crate::usage::PartitionAggregate>> {
		let key = Key::Usage(crate::lmdb::usage::Key::Aggregate {
			account: account.clone(),
			partition,
			period,
		});
		let key = Self::pack(subspace, &key);
		let aggregate = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the usage aggregate"))?
			.map(crate::usage::deserialize_aggregate)
			.transpose()?;

		Ok(aggregate)
	}

	pub(in crate::lmdb) fn contains_usage_aggregation_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) -> tg::Result<bool> {
		let key = Key::Usage(crate::lmdb::usage::Key::Aggregation {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let contains = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the usage aggregation"))?
			.is_some();

		Ok(contains)
	}

	pub(in crate::lmdb) fn put_usage_aggregate_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
		aggregate: crate::usage::PartitionAggregate,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::lmdb::usage::Key::Aggregate {
			account: account.clone(),
			partition,
			period,
		});
		let key = Self::pack(subspace, &key);
		let value = crate::usage::serialize_aggregate(&aggregate);
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the usage aggregate"))?;

		Ok(())
	}

	pub(in crate::lmdb) fn put_usage_aggregation_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::lmdb::usage::Key::Aggregation {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the usage aggregation"))?;

		Ok(())
	}

	fn clear_usage_aggregation_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) -> tg::Result<()> {
		let key = Key::Usage(crate::lmdb::usage::Key::Aggregation {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete the usage aggregation"))?;

		Ok(())
	}

	fn get_usage_deltas_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) -> tg::Result<Deltas> {
		let account = account.id().to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				Kind::UsageDelta.to_i32().unwrap(),
				partition,
				hour,
				account.as_ref(),
			),
		);
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the usage deltas"))?;
		let mut deltas = Deltas::default();
		for entry in entries {
			let (key, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read a usage delta"))?;
			let Key::Usage(crate::lmdb::usage::Key::Delta { kind, .. }) =
				Self::unpack(subspace, key)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = i64::from_le_bytes(
				value
					.try_into()
					.map_err(|_| tg::error!("invalid usage delta"))?,
			);
			let value = i128::from(value);
			let target = match kind {
				crate::usage::DeltaKind::ObjectCount => &mut deltas.object_count,
				crate::usage::DeltaKind::ObjectSize => &mut deltas.object_size,
				crate::usage::DeltaKind::ProcessCount => &mut deltas.process_count,
				crate::usage::DeltaKind::SandboxCount => &mut deltas.sandbox_count,
				crate::usage::DeltaKind::SandboxCpu => &mut deltas.sandbox_cpu,
				crate::usage::DeltaKind::SandboxMemory => &mut deltas.sandbox_memory,
			};
			*target = target
				.checked_add(value)
				.ok_or_else(|| tg::error!("the usage delta overflowed"))?;
		}

		Ok(deltas)
	}

	fn try_get_usage_aggregation_for_account_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		end_hour: i64,
	) -> tg::Result<Option<i64>> {
		let prefix = Self::pack(
			subspace,
			&(Kind::UsageAggregation.to_i32().unwrap(), partition),
		);
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate usage aggregations"))?;
		for entry in entries {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a usage aggregation"))?;
			let Key::Usage(crate::lmdb::usage::Key::Aggregation {
				account: candidate,
				hour,
				..
			}) = Self::unpack(subspace, key)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			if hour >= end_hour {
				break;
			}
			if candidate == *account {
				return Ok(Some(hour));
			}
		}

		Ok(None)
	}

	fn sum_usage_children_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
	) -> tg::Result<crate::usage::PartitionAggregate> {
		let mut aggregate = crate::usage::PartitionAggregate::default();
		for child in crate::usage::children(period)? {
			let child = Self::try_get_usage_aggregate_with_transaction(
				db,
				subspace,
				transaction,
				account,
				partition,
				child,
			)?
			.unwrap_or_default();
			aggregate.checked_add(child)?;
		}

		Ok(aggregate)
	}
}

fn apply_delta(value: i128, delta: i128) -> tg::Result<i128> {
	let value = value
		.checked_add(delta)
		.ok_or_else(|| tg::error!("the usage value overflowed"))?;

	Ok(value)
}

fn storage_changed(
	left: crate::usage::PartitionAggregate,
	right: crate::usage::PartitionAggregate,
) -> bool {
	left.object_count != right.object_count
		|| left.object_size != right.object_size
		|| left.process_count != right.process_count
}
