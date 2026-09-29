use {
	crate::{Index, Key, Kind, Request, Response},
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

	pub(crate) async fn aggregate_usage_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::aggregate::Arg,
	) -> tg::Result<ControlFlow<tangram_index::usage::aggregate::Output, fdb::FdbError>> {
		let current_hour = arg.now.as_second().div_euclid(60 * 60) * 60 * 60;
		let mut candidates = Vec::new();
		for partition in arg.partition_start..arg.partition_end {
			let start = Self::pack(
				subspace,
				&(Kind::UsageAggregation.to_i32().unwrap(), partition),
			);
			let end = Self::pack(
				subspace,
				&(
					Kind::UsageAggregation.to_i32().unwrap(),
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
				let Some(entry) = crate::retry!(result) else {
					break;
				};
				let Key::Usage(crate::usage::Key::Aggregation {
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
			let value = crate::propagate!(
				Self::aggregate_usage_for_account_with_transaction(
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
		let output = tangram_index::usage::aggregate::Output { count };

		Ok(ControlFlow::Break(output))
	}

	pub(crate) async fn aggregate_usage_for_account_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		partition: u64,
		end_hour: i64,
		limit: Option<usize>,
	) -> tg::Result<ControlFlow<usize, fdb::FdbError>> {
		let mut count = 0;
		loop {
			if limit.is_some_and(|limit| count == limit) {
				break;
			}
			let hour = crate::propagate!(
				Self::try_get_usage_aggregation_for_account_with_transaction(
					txn, subspace, account, partition, end_hour,
				)
				.await
			);
			let Some(hour) = hour else {
				break;
			};
			crate::propagate!(
				Self::aggregate_usage_hour_with_transaction(
					txn, subspace, account, hour, partition, true,
				)
				.await
			);
			count += 1;
		}

		Ok(ControlFlow::Break(count))
	}

	pub(crate) async fn aggregate_usage_hour_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		hour: i64,
		partition: u64,
		clear_aggregation: bool,
	) -> tg::Result<ControlFlow<tangram_index::usage::PartitionAggregate, fdb::FdbError>> {
		let period = tangram_index::usage::Period::from_kind_and_start(
			tangram_index::usage::PeriodKind::Hour,
			hour,
		)?;
		let old = crate::propagate!(
			Self::try_get_usage_aggregate_with_transaction(
				txn, subspace, account, partition, period,
			)
			.await
		)
		.unwrap_or_default();
		let previous = match hour.checked_sub(60 * 60) {
			Some(start) => {
				let period = tangram_index::usage::Period::from_kind_and_start(
					tangram_index::usage::PeriodKind::Hour,
					start,
				)?;
				crate::propagate!(
					Self::try_get_usage_aggregate_with_transaction(
						txn, subspace, account, partition, period,
					)
					.await
				)
				.unwrap_or_default()
			},
			None => tangram_index::usage::PartitionAggregate::default(),
		};
		let deltas = crate::propagate!(
			Self::get_usage_deltas_with_transaction(txn, subspace, account, hour, partition,).await
		);
		let sandbox_cpu = u128::try_from(deltas.sandbox_cpu)
			.map_err(|_| tg::error!("the sandbox CPU usage is out of range"))?;
		let sandbox_memory = u128::try_from(deltas.sandbox_memory)
			.map_err(|_| tg::error!("the sandbox memory usage is out of range"))?;
		let aggregate = tangram_index::usage::PartitionAggregate {
			object_count: apply_delta(previous.object_count, deltas.object_count)?,
			object_size: apply_delta(previous.object_size, deltas.object_size)?,
			process_count: apply_delta(previous.process_count, deltas.process_count)?,
			sandbox_count: deltas.sandbox_count,
			sandbox_cpu,
			sandbox_memory,
		};
		if !clear_aggregation {
			return Ok(ControlFlow::Break(aggregate));
		}
		Self::put_usage_aggregate_with_transaction(
			txn, subspace, account, partition, period, aggregate,
		);
		Self::clear_usage_aggregation_with_transaction(txn, subspace, account, hour, partition);

		let changed = aggregate != old;
		if storage_changed(old, aggregate) {
			let next = hour
				.checked_add(60 * 60)
				.ok_or_else(|| tg::error!("the usage hour overflowed"))?;
			Self::put_usage_aggregation_with_transaction(txn, subspace, account, next, partition);
		}
		let day = tangram_index::usage::Period::containing(
			tangram_index::usage::PeriodKind::Day,
			period.start(),
		);
		let closing_hour = tangram_index::usage::closing_hour(day)?;
		if hour == closing_hour {
			crate::propagate!(
				Self::aggregate_usage_day_with_transaction(
					txn, subspace, account, partition, day, hour,
				)
				.await
			);
		} else if changed {
			Self::put_usage_aggregation_with_transaction(
				txn,
				subspace,
				account,
				closing_hour,
				partition,
			);
		}

		Ok(ControlFlow::Break(aggregate))
	}

	pub(crate) async fn aggregate_usage_day_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		partition: u64,
		period: tangram_index::usage::Period,
		hour: i64,
	) -> tg::Result<ControlFlow<tangram_index::usage::PartitionAggregate, fdb::FdbError>> {
		let old = crate::propagate!(
			Self::try_get_usage_aggregate_with_transaction(
				txn, subspace, account, partition, period,
			)
			.await
		)
		.unwrap_or_default();
		let aggregate = crate::propagate!(
			Self::sum_usage_children_with_transaction(txn, subspace, account, partition, period,)
				.await
		);
		Self::put_usage_aggregate_with_transaction(
			txn, subspace, account, partition, period, aggregate,
		);

		for kind in [
			tangram_index::usage::PeriodKind::Week,
			tangram_index::usage::PeriodKind::Month,
		] {
			let parent = tangram_index::usage::Period::containing(kind, period.start());
			let closing_hour = tangram_index::usage::closing_hour(parent)?;
			if hour == closing_hour {
				crate::propagate!(
					Self::aggregate_usage_parent_with_transaction(
						txn, subspace, account, partition, parent,
					)
					.await
				);
			} else if aggregate != old {
				Self::put_usage_aggregation_with_transaction(
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

	pub(crate) async fn aggregate_usage_parent_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		partition: u64,
		period: tangram_index::usage::Period,
	) -> tg::Result<ControlFlow<tangram_index::usage::PartitionAggregate, fdb::FdbError>> {
		let aggregate = crate::propagate!(
			Self::sum_usage_children_with_transaction(txn, subspace, account, partition, period,)
				.await
		);
		Self::put_usage_aggregate_with_transaction(
			txn, subspace, account, partition, period, aggregate,
		);

		Ok(ControlFlow::Break(aggregate))
	}

	pub(crate) async fn try_get_usage_aggregate_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		partition: u64,
		period: tangram_index::usage::Period,
	) -> tg::Result<ControlFlow<Option<tangram_index::usage::PartitionAggregate>, fdb::FdbError>> {
		let key = Key::Usage(crate::usage::Key::Aggregate {
			account: account.clone(),
			partition,
			period,
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let aggregate = crate::retry!(result)
			.map(|bytes| tangram_index::usage::deserialize_aggregate(&bytes))
			.transpose()?;

		Ok(ControlFlow::Break(aggregate))
	}

	pub(crate) async fn contains_usage_aggregation_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		hour: i64,
		partition: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let key = Key::Usage(crate::usage::Key::Aggregation {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let contains = crate::retry!(result).is_some();

		Ok(ControlFlow::Break(contains))
	}

	pub(crate) fn put_usage_aggregate_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		partition: u64,
		period: tangram_index::usage::Period,
		aggregate: tangram_index::usage::PartitionAggregate,
	) {
		let key = Key::Usage(crate::usage::Key::Aggregate {
			account: account.clone(),
			partition,
			period,
		});
		let key = Self::pack(subspace, &key);
		let value = tangram_index::usage::serialize_aggregate(&aggregate);
		txn.set(&key, &value);
	}

	pub(crate) fn put_usage_aggregation_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		hour: i64,
		partition: u64,
	) {
		let key = Key::Usage(crate::usage::Key::Aggregation {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);
	}

	fn clear_usage_aggregation_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		hour: i64,
		partition: u64,
	) {
		let key = Key::Usage(crate::usage::Key::Aggregation {
			account: account.clone(),
			hour,
			partition,
		});
		let key = Self::pack(subspace, &key);
		txn.clear(&key);
	}

	async fn get_usage_deltas_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
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
			let Some(entry) = crate::retry!(result) else {
				break;
			};
			let Key::Usage(crate::usage::Key::Delta { kind, .. }) =
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
				tangram_index::usage::DeltaKind::ObjectCount => &mut deltas.object_count,
				tangram_index::usage::DeltaKind::ObjectSize => &mut deltas.object_size,
				tangram_index::usage::DeltaKind::ProcessCount => &mut deltas.process_count,
				tangram_index::usage::DeltaKind::SandboxCount => &mut deltas.sandbox_count,
				tangram_index::usage::DeltaKind::SandboxCpu => &mut deltas.sandbox_cpu,
				tangram_index::usage::DeltaKind::SandboxMemory => &mut deltas.sandbox_memory,
			};
			*target = target
				.checked_add(value)
				.ok_or_else(|| tg::error!("the usage delta overflowed"))?;
		}

		Ok(ControlFlow::Break(deltas))
	}

	async fn try_get_usage_aggregation_for_account_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		partition: u64,
		end_hour: i64,
	) -> tg::Result<ControlFlow<Option<i64>, fdb::FdbError>> {
		let start = Self::pack(
			subspace,
			&(Kind::UsageAggregation.to_i32().unwrap(), partition),
		);
		let end = Self::pack(
			subspace,
			&(
				Kind::UsageAggregation.to_i32().unwrap(),
				partition,
				end_hour,
			),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::Iterator,
			..fdb::RangeOption::from((start.as_slice(), end.as_slice()))
		};
		let mut entries = txn.get_ranges_keyvalues(range, false);
		loop {
			let result = entries.try_next().await;
			let Some(entry) = crate::retry!(result) else {
				break;
			};
			let Key::Usage(crate::usage::Key::Aggregation {
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
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		partition: u64,
		period: tangram_index::usage::Period,
	) -> tg::Result<ControlFlow<tangram_index::usage::PartitionAggregate, fdb::FdbError>> {
		let mut aggregate = tangram_index::usage::PartitionAggregate::default();
		for child in tangram_index::usage::children(period)? {
			let child = crate::propagate!(
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
	left: tangram_index::usage::PartitionAggregate,
	right: tangram_index::usage::PartitionAggregate,
) -> bool {
	left.object_count != right.object_count
		|| left.object_size != right.object_size
		|| left.process_count != right.process_count
}
