use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn expire_usage_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::expire::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<tangram_index::usage::expire::Output, fdb::FdbError>> {
		if arg.partition_start > arg.partition_end || arg.partition_end > partition_total {
			return Err(tg::error!(
				"the usage expiration partition range is invalid"
			));
		}
		let mut keys = Vec::new();
		let mut pending = false;
		let mut unavailable = BTreeMap::new();
		crate::propagate!(
			Self::find_usage_delta_candidates(txn, subspace, arg, &mut keys, &mut pending).await
		);
		if keys.len() < arg.batch_size {
			crate::propagate!(
				Self::find_usage_aggregate_candidates(
					txn,
					subspace,
					arg,
					&mut keys,
					&mut pending,
					&mut unavailable,
				)
				.await
			);
		}
		for ((account, kind, partition), through) in unavailable {
			Self::mark_usage_unavailable_with_transaction(
				txn, subspace, &account, kind, partition, through,
			);
		}
		for key in &keys {
			txn.clear(key);
		}
		let output = tangram_index::usage::expire::Output {
			deleted: keys.len(),
			done: keys.is_empty() && !pending,
		};

		Ok(ControlFlow::Break(output))
	}

	async fn find_usage_delta_candidates(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::expire::Arg,
		keys: &mut Vec<Vec<u8>>,
		pending: &mut bool,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let cutoff = arg
			.now
			.checked_sub(arg.delta_time_to_live)
			.unwrap_or(jiff::Timestamp::MIN);
		for partition in arg.partition_start..arg.partition_end {
			let start = Self::pack(subspace, &(Kind::UsageDelta.to_i32().unwrap(), partition));
			let end = Self::pack(
				subspace,
				&(
					Kind::UsageDelta.to_i32().unwrap(),
					partition,
					cutoff.as_second(),
				),
			);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::Iterator,
				..fdb::RangeOption::from((start.as_slice(), end.as_slice()))
			};
			let mut entries = txn.get_ranges_keyvalues(range, false);
			while keys.len() < arg.batch_size {
				let result = entries.try_next().await;
				let Some(entry) = crate::retry!(result) else {
					break;
				};
				let Key::Usage(crate::usage::Key::Delta {
					account,
					hour,
					partition,
					..
				}) = Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let period = tangram_index::usage::Period::from_kind_and_start(
					tangram_index::usage::PeriodKind::Hour,
					hour,
				)?;
				if period.end() > cutoff {
					break;
				}
				let aggregating = crate::propagate!(
					Self::contains_usage_aggregation_with_transaction(
						txn, subspace, &account, hour, partition,
					)
					.await
				);
				if aggregating {
					let current_hour = arg.now.as_second().div_euclid(60 * 60) * 60 * 60;
					*pending |= hour < current_hour;
				} else {
					keys.push(entry.key().to_vec());
				}
			}
			if keys.len() == arg.batch_size {
				break;
			}
		}

		Ok(ControlFlow::Break(()))
	}

	async fn find_usage_aggregate_candidates(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::usage::expire::Arg,
		keys: &mut Vec<Vec<u8>>,
		pending: &mut bool,
		unavailable: &mut BTreeMap<
			(
				tangram_index::usage::Account,
				tangram_index::usage::PeriodKind,
				u64,
			),
			i64,
		>,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for partition in arg.partition_start..arg.partition_end {
			for kind in [
				tangram_index::usage::PeriodKind::Hour,
				tangram_index::usage::PeriodKind::Day,
				tangram_index::usage::PeriodKind::Week,
				tangram_index::usage::PeriodKind::Month,
			] {
				let time_to_live = match kind {
					tangram_index::usage::PeriodKind::Hour => arg.hour_time_to_live,
					tangram_index::usage::PeriodKind::Day => arg.day_time_to_live,
					tangram_index::usage::PeriodKind::Week => arg.week_time_to_live,
					tangram_index::usage::PeriodKind::Month => arg.month_time_to_live,
				};
				let cutoff = arg
					.now
					.checked_sub(time_to_live)
					.unwrap_or(jiff::Timestamp::MIN);
				let start = Self::pack(
					subspace,
					&(
						Kind::UsageAggregate.to_i32().unwrap(),
						partition,
						i32::from(kind as u8),
					),
				);
				let end = Self::pack(
					subspace,
					&(
						Kind::UsageAggregate.to_i32().unwrap(),
						partition,
						i32::from(kind as u8),
						cutoff.as_second(),
					),
				);
				let range = fdb::RangeOption {
					mode: fdb::options::StreamingMode::Iterator,
					..fdb::RangeOption::from((start.as_slice(), end.as_slice()))
				};
				let mut entries = txn.get_ranges_keyvalues(range, false);
				while keys.len() < arg.batch_size {
					let result = entries.try_next().await;
					let Some(entry) = crate::retry!(result) else {
						break;
					};
					let Key::Usage(crate::usage::Key::Aggregate {
						account,
						partition,
						period,
					}) = Self::unpack(subspace, entry.key())?
					else {
						return Err(tg::error!("unexpected key type"));
					};
					if period.end() > cutoff {
						break;
					}
					let current_hour = arg.now.as_second().div_euclid(60 * 60) * 60 * 60;
					let (dependency, eligible) = match period {
						tangram_index::usage::Period::Hour(_) => {
							let next_hour = period
								.start()
								.as_second()
								.checked_add(60 * 60)
								.ok_or_else(|| tg::error!("the usage hour overflowed"))?;
							let day = tangram_index::usage::Period::containing(
								tangram_index::usage::PeriodKind::Day,
								period.start(),
							);
							let closing_hour = tangram_index::usage::closing_hour(day)?;
							let next = crate::propagate!(
								Self::contains_usage_aggregation_with_transaction(
									txn, subspace, &account, next_hour, partition,
								)
								.await
							);
							let closing = crate::propagate!(
								Self::contains_usage_aggregation_with_transaction(
									txn,
									subspace,
									&account,
									closing_hour,
									partition,
								)
								.await
							);
							let dependency = next || closing;
							let eligible = (next && next_hour < current_hour)
								|| (closing && closing_hour < current_hour);
							(dependency, eligible)
						},
						tangram_index::usage::Period::Day(_) => {
							let mut dependency = false;
							let mut eligible = false;
							for kind in [
								tangram_index::usage::PeriodKind::Week,
								tangram_index::usage::PeriodKind::Month,
							] {
								let parent =
									tangram_index::usage::Period::containing(kind, period.start());
								let closing_hour = tangram_index::usage::closing_hour(parent)?;
								let contains = crate::propagate!(
									Self::contains_usage_aggregation_with_transaction(
										txn,
										subspace,
										&account,
										closing_hour,
										partition,
									)
									.await
								);
								dependency |= contains;
								eligible |= contains && closing_hour < current_hour;
							}
							(dependency, eligible)
						},
						tangram_index::usage::Period::Month(_)
						| tangram_index::usage::Period::Week(_) => (false, false),
					};
					if dependency {
						*pending |= eligible;
					} else {
						let through = period.end().as_second();
						unavailable
							.entry((account, period.kind(), partition))
							.and_modify(|value| *value = (*value).max(through))
							.or_insert(through);
						keys.push(entry.key().to_vec());
					}
				}
				if keys.len() == arg.batch_size {
					return Ok(ControlFlow::Break(()));
				}
			}
		}

		Ok(ControlFlow::Break(()))
	}
}
