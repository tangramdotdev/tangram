use {
	crate::fdb::{Index, Key, Kind, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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
	pub async fn compact_usage(
		&self,
		arg: crate::usage::compact::Arg,
	) -> tg::Result<crate::usage::compact::Output> {
		let response = self.send_write_request(Request::CompactUsage(arg)).await?;
		let Response::CompactUsageOutput(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

	pub(crate) async fn compact_usage_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::compact::Arg,
	) -> tg::Result<ControlFlow<crate::usage::compact::Output, fdb::FdbError>> {
		let current_hour = arg.now.as_second().div_euclid(60 * 60) * 60 * 60;
		let mut candidates = Vec::new();
		for partition in arg.partition_start..arg.partition_end {
			let start = Self::pack(
				subspace,
				&(Kind::UsageCompaction.to_i32().unwrap(), partition),
			);
			let end = Self::pack(
				subspace,
				&(
					Kind::UsageCompaction.to_i32().unwrap(),
					partition,
					current_hour,
				),
			);
			let range = fdb::RangeOption {
				limit: Some(arg.batch_size.saturating_sub(candidates.len())),
				mode: fdb::options::StreamingMode::Iterator,
				..fdb::RangeOption::from((start.as_slice(), end.as_slice()))
			};
			let mut entries = txn.get_ranges_keyvalues(range, false);
			while candidates.len() < arg.batch_size {
				let result = entries.try_next().await;
				let Some(entry) = crate::fdb::retry!(result) else {
					break;
				};
				let Key::Usage(crate::fdb::usage::Key::Compaction {
					account,
					hour,
					partition,
				}) = Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				candidates.push((account, hour, partition));
			}
			if candidates.len() == arg.batch_size {
				break;
			}
		}

		let mut count = 0;
		for (account, _, partition) in &candidates {
			let limit = arg.batch_size - count;
			let value = crate::fdb::propagate!(
				Self::aggregate_usage_compactions_for_account_with_transaction(
					txn,
					subspace,
					account,
					*partition,
					current_hour,
					Some(limit),
				)
				.await
			);
			count += value;
			if count == arg.batch_size {
				break;
			}
		}
		let output = crate::usage::compact::Output { count };

		Ok(ControlFlow::Break(output))
	}

	pub(in crate::fdb) async fn aggregate_usage_compactions_for_account_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		partition: u64,
		end_hour: i64,
		limit: Option<usize>,
	) -> tg::Result<ControlFlow<usize, fdb::FdbError>> {
		let mut count = 0;
		loop {
			if limit.is_some_and(|limit| count == limit) {
				break;
			}
			let hour = crate::fdb::propagate!(
				Self::try_get_usage_compaction_for_account_with_transaction(
					txn, subspace, account, partition, end_hour,
				)
				.await
			);
			let Some(hour) = hour else {
				break;
			};
			crate::fdb::propagate!(
				Self::aggregate_usage_hour_with_transaction(
					txn, subspace, account, hour, partition, true,
				)
				.await
			);
			count += 1;
		}

		Ok(ControlFlow::Break(count))
	}

	pub(in crate::fdb) async fn aggregate_usage_hour_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
		clear_compaction: bool,
	) -> tg::Result<ControlFlow<crate::usage::PartitionAggregate, fdb::FdbError>> {
		let period =
			crate::usage::Period::from_kind_and_start(crate::usage::PeriodKind::Hour, hour)?;
		let old = crate::fdb::propagate!(
			Self::try_get_usage_aggregate_with_transaction(
				txn, subspace, account, partition, period,
			)
			.await
		)
		.unwrap_or_default();
		let previous = match hour.checked_sub(60 * 60) {
			Some(start) => {
				let period = crate::usage::Period::from_kind_and_start(
					crate::usage::PeriodKind::Hour,
					start,
				)?;
				crate::fdb::propagate!(
					Self::try_get_usage_aggregate_with_transaction(
						txn, subspace, account, partition, period,
					)
					.await
				)
				.unwrap_or_default()
			},
			None => crate::usage::PartitionAggregate::default(),
		};
		let deltas = crate::fdb::propagate!(
			Self::get_usage_deltas_with_transaction(txn, subspace, account, hour, partition,).await
		);
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
		if !clear_compaction {
			return Ok(ControlFlow::Break(aggregate));
		}
		Self::put_usage_aggregate_with_transaction(
			txn, subspace, account, partition, period, aggregate,
		);
		Self::clear_usage_compaction_with_transaction(txn, subspace, account, hour, partition);

		let changed = aggregate != old;
		if storage_changed(old, aggregate) {
			let next = hour
				.checked_add(60 * 60)
				.ok_or_else(|| tg::error!("the usage hour overflowed"))?;
			Self::put_usage_compaction_with_transaction(txn, subspace, account, next, partition);
		}
		let day = crate::usage::Period::containing(crate::usage::PeriodKind::Day, period.start());
		let closing_hour = crate::usage::closing_hour(day)?;
		if hour == closing_hour {
			crate::fdb::propagate!(
				Self::aggregate_usage_day_with_transaction(
					txn, subspace, account, partition, day, hour,
				)
				.await
			);
		} else if changed {
			Self::put_usage_compaction_with_transaction(
				txn,
				subspace,
				account,
				closing_hour,
				partition,
			);
		}

		Ok(ControlFlow::Break(aggregate))
	}

	pub(in crate::fdb) async fn aggregate_usage_day_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
		hour: i64,
	) -> tg::Result<ControlFlow<crate::usage::PartitionAggregate, fdb::FdbError>> {
		let old = crate::fdb::propagate!(
			Self::try_get_usage_aggregate_with_transaction(
				txn, subspace, account, partition, period,
			)
			.await
		)
		.unwrap_or_default();
		let aggregate = crate::fdb::propagate!(
			Self::sum_usage_children_with_transaction(txn, subspace, account, partition, period,)
				.await
		);
		Self::put_usage_aggregate_with_transaction(
			txn, subspace, account, partition, period, aggregate,
		);

		for kind in [
			crate::usage::PeriodKind::Week,
			crate::usage::PeriodKind::Month,
		] {
			let parent = crate::usage::Period::containing(kind, period.start());
			let closing_hour = crate::usage::closing_hour(parent)?;
			if hour == closing_hour {
				crate::fdb::propagate!(
					Self::aggregate_usage_parent_with_transaction(
						txn, subspace, account, partition, parent,
					)
					.await
				);
			} else if aggregate != old {
				Self::put_usage_compaction_with_transaction(
					txn,
					subspace,
					account,
					closing_hour,
					partition,
				);
			}
		}

		Ok(ControlFlow::Break(aggregate))
	}

	pub(in crate::fdb) async fn aggregate_usage_parent_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
	) -> tg::Result<ControlFlow<crate::usage::PartitionAggregate, fdb::FdbError>> {
		let aggregate = crate::fdb::propagate!(
			Self::sum_usage_children_with_transaction(txn, subspace, account, partition, period,)
				.await
		);
		Self::put_usage_aggregate_with_transaction(
			txn, subspace, account, partition, period, aggregate,
		);

		Ok(ControlFlow::Break(aggregate))
	}

	pub(in crate::fdb) async fn try_get_usage_aggregate_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
	) -> tg::Result<ControlFlow<Option<crate::usage::PartitionAggregate>, fdb::FdbError>> {
		let key = Key::Usage(crate::fdb::usage::Key::Aggregate {
			account: account.clone(),
			partition,
			period,
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let aggregate = crate::fdb::retry!(result)
			.map(|bytes| crate::usage::deserialize_aggregate(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(aggregate))
	}

	pub(in crate::fdb) async fn contains_usage_compaction_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let key = Key::Usage(crate::fdb::usage::Key::Compaction {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let contains = crate::fdb::retry!(result).is_some();

		Ok(ControlFlow::Break(contains))
	}

	pub(in crate::fdb) fn put_usage_aggregate_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
		aggregate: crate::usage::PartitionAggregate,
	) {
		let key = Key::Usage(crate::fdb::usage::Key::Aggregate {
			account: account.clone(),
			partition,
			period,
		});
		let key = Self::pack(subspace, &key);
		let value = crate::usage::serialize_aggregate(&aggregate);
		txn.set(&key, &value);
	}

	pub(in crate::fdb) fn put_usage_compaction_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) {
		let key = Key::Usage(crate::fdb::usage::Key::Compaction {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);
	}

	fn clear_usage_compaction_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) {
		let key = Key::Usage(crate::fdb::usage::Key::Compaction {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.clear(&key);
	}

	async fn get_usage_deltas_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		hour: i64,
		partition: u64,
	) -> tg::Result<ControlFlow<Deltas, fdb::FdbError>> {
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
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let mut entries = txn.get_ranges_keyvalues(range, false);
		let mut deltas = Deltas::default();
		loop {
			let result = entries.try_next().await;
			let Some(entry) = crate::fdb::retry!(result) else {
				break;
			};
			let Key::Usage(crate::fdb::usage::Key::Delta { kind, .. }) =
				Self::unpack(subspace, entry.key())?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = i64::from_le_bytes(
				entry
					.value()
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

		Ok(ControlFlow::Break(deltas))
	}

	async fn try_get_usage_compaction_for_account_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		partition: u64,
		end_hour: i64,
	) -> tg::Result<ControlFlow<Option<i64>, fdb::FdbError>> {
		let start = Self::pack(
			subspace,
			&(Kind::UsageCompaction.to_i32().unwrap(), partition),
		);
		let end = Self::pack(
			subspace,
			&(Kind::UsageCompaction.to_i32().unwrap(), partition, end_hour),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::Iterator,
			..fdb::RangeOption::from((start.as_slice(), end.as_slice()))
		};
		let mut entries = txn.get_ranges_keyvalues(range, false);
		loop {
			let result = entries.try_next().await;
			let Some(entry) = crate::fdb::retry!(result) else {
				break;
			};
			let Key::Usage(crate::fdb::usage::Key::Compaction {
				account: candidate,
				hour,
				..
			}) = Self::unpack(subspace, entry.key())?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			if candidate == *account {
				return Ok(ControlFlow::Break(Some(hour)));
			}
		}

		Ok(ControlFlow::Break(None))
	}

	async fn sum_usage_children_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
	) -> tg::Result<ControlFlow<crate::usage::PartitionAggregate, fdb::FdbError>> {
		let mut aggregate = crate::usage::PartitionAggregate::default();
		for child in crate::usage::children(period)? {
			let child = crate::fdb::propagate!(
				Self::try_get_usage_aggregate_with_transaction(
					txn, subspace, account, partition, child,
				)
				.await
			)
			.unwrap_or_default();
			aggregate.checked_add(child)?;
		}

		Ok(ControlFlow::Break(aggregate))
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
