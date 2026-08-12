use {
	crate::fdb::{Index, Key, Kind, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
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
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::clean::Arg,
		partition_total: u64,
	) -> tg::Result<crate::usage::clean::Output> {
		let mut keys = Vec::new();
		let mut pending = false;
		Self::find_usage_delta_candidates(
			txn,
			subspace,
			arg,
			partition_total,
			&mut keys,
			&mut pending,
		)
		.await?;
		if keys.len() < arg.batch_size {
			Self::find_usage_aggregate_candidates(
				txn,
				subspace,
				arg,
				partition_total,
				&mut keys,
				&mut pending,
			)
			.await?;
		}
		for key in &keys {
			txn.clear(key);
		}
		let output = crate::usage::clean::Output {
			deleted: keys.len(),
			done: keys.is_empty() && !pending,
		};

		Ok(output)
	}

	async fn find_usage_delta_candidates(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::clean::Arg,
		partition_total: u64,
		keys: &mut Vec<Vec<u8>>,
		pending: &mut bool,
	) -> tg::Result<()> {
		let cutoff = arg
			.now
			.checked_sub(arg.delta_time_to_live)
			.unwrap_or(jiff::Timestamp::MIN);
		for partition in 0..partition_total {
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
				let Some(entry) = entries
					.try_next()
					.await
					.map_err(|error| tg::error!(!error, "failed to get usage deltas"))?
				else {
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
				let compacting = Self::contains_usage_compaction_with_transaction(
					txn, subspace, &account, hour, partition,
				)
				.await?;
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

		Ok(())
	}

	async fn find_usage_aggregate_candidates(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::usage::clean::Arg,
		partition_total: u64,
		keys: &mut Vec<Vec<u8>>,
		pending: &mut bool,
	) -> tg::Result<()> {
		for partition in 0..partition_total {
			for kind in [
				crate::usage::PeriodKind::Hour,
				crate::usage::PeriodKind::Day,
				crate::usage::PeriodKind::Week,
				crate::usage::PeriodKind::Month,
			] {
				let time_to_live = match kind {
					crate::usage::PeriodKind::Day => arg.day_time_to_live,
					crate::usage::PeriodKind::Hour => arg.hour_time_to_live,
					crate::usage::PeriodKind::Month => arg.month_time_to_live,
					crate::usage::PeriodKind::Week => arg.week_time_to_live,
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
					let Some(entry) = entries
						.try_next()
						.await
						.map_err(|error| tg::error!(!error, "failed to get usage aggregates"))?
					else {
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
							let next = Self::contains_usage_compaction_with_transaction(
								txn, subspace, &account, next_hour, partition,
							)
							.await?;
							let closing = Self::contains_usage_compaction_with_transaction(
								txn,
								subspace,
								&account,
								closing_hour,
								partition,
							)
							.await?;
							let dependency = next || closing;
							let eligible = (next && next_hour < current_hour)
								|| (closing && closing_hour < current_hour);
							(dependency, eligible)
						},
						crate::usage::Period::Day(_) => {
							let mut dependency = false;
							let mut eligible = false;
							for kind in [
								crate::usage::PeriodKind::Month,
								crate::usage::PeriodKind::Week,
							] {
								let parent = crate::usage::Period::containing(kind, period.start());
								let closing_hour = crate::usage::closing_hour(parent)?;
								let contains = Self::contains_usage_compaction_with_transaction(
									txn,
									subspace,
									&account,
									closing_hour,
									partition,
								)
								.await?;
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
						keys.push(entry.key().to_vec());
					}
				}
				if keys.len() == arg.batch_size {
					return Ok(());
				}
			}
		}

		Ok(())
	}
}
