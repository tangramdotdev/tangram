use {
	crate::fdb::{Index, Key, Kind, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn clean_usage(
		&self,
		arg: crate::usage::clean::Arg,
	) -> tg::Result<crate::usage::clean::Output> {
		let response = self.send_write_request(Request::CleanUsage(arg)).await?;
		let Response::CleanUsageOutput(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

	pub(crate) async fn clean_usage_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::clean::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<crate::usage::clean::Output, fdb::FdbError>> {
		if arg.partition_start > arg.partition_end || arg.partition_end > partition_total {
			return Err(tg::error!("the usage cleaning partition range is invalid"));
		}
		let mut keys = Vec::new();
		let mut pending = false;
		let mut unavailable = BTreeMap::new();
		crate::fdb::propagate!(
			Self::find_usage_delta_candidates(txn, subspace, arg, &mut keys, &mut pending).await
		);
		if keys.len() < arg.batch_size {
			crate::fdb::propagate!(
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
		let output = crate::usage::clean::Output {
			deleted: keys.len(),
			done: keys.is_empty() && !pending,
		};

		Ok(ControlFlow::Break(output))
	}

	async fn find_usage_delta_candidates(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::clean::Arg,
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
				let Some(entry) = crate::fdb::retry!(result) else {
					break;
				};
				let Key::Usage(crate::fdb::usage::Key::Delta {
					account,
					hour,
					partition,
					..
				}) = Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let period = crate::usage::Period::from_kind_and_start(
					crate::usage::PeriodKind::Hour,
					hour,
				)?;
				if period.end() > cutoff {
					break;
				}
				let compacting = crate::fdb::propagate!(
					Self::contains_usage_compaction_with_transaction(
						txn, subspace, &account, hour, partition,
					)
					.await
				);
				if compacting {
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
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::clean::Arg,
		keys: &mut Vec<Vec<u8>>,
		pending: &mut bool,
		unavailable: &mut BTreeMap<(crate::usage::Account, crate::usage::PeriodKind, u64), i64>,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for partition in arg.partition_start..arg.partition_end {
			for kind in [
				crate::usage::PeriodKind::Hour,
				crate::usage::PeriodKind::Day,
				crate::usage::PeriodKind::Week,
				crate::usage::PeriodKind::Month,
			] {
				let time_to_live = match kind {
					crate::usage::PeriodKind::Hour => arg.hour_time_to_live,
					crate::usage::PeriodKind::Day => arg.day_time_to_live,
					crate::usage::PeriodKind::Week => arg.week_time_to_live,
					crate::usage::PeriodKind::Month => arg.month_time_to_live,
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
					let Some(entry) = crate::fdb::retry!(result) else {
						break;
					};
					let Key::Usage(crate::fdb::usage::Key::Aggregate {
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
						crate::usage::Period::Hour(_) => {
							let next_hour = period
								.start()
								.as_second()
								.checked_add(60 * 60)
								.ok_or_else(|| tg::error!("the usage hour overflowed"))?;
							let day = crate::usage::Period::containing(
								crate::usage::PeriodKind::Day,
								period.start(),
							);
							let closing_hour = crate::usage::closing_hour(day)?;
							let next = crate::fdb::propagate!(
								Self::contains_usage_compaction_with_transaction(
									txn, subspace, &account, next_hour, partition,
								)
								.await
							);
							let closing = crate::fdb::propagate!(
								Self::contains_usage_compaction_with_transaction(
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
						crate::usage::Period::Day(_) => {
							let mut dependency = false;
							let mut eligible = false;
							for kind in [
								crate::usage::PeriodKind::Week,
								crate::usage::PeriodKind::Month,
							] {
								let parent = crate::usage::Period::containing(kind, period.start());
								let closing_hour = crate::usage::closing_hour(parent)?;
								let contains = crate::fdb::propagate!(
									Self::contains_usage_compaction_with_transaction(
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
						crate::usage::Period::Month(_) | crate::usage::Period::Week(_) => {
							(false, false)
						},
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
