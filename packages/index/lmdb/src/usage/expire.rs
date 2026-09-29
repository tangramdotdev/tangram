use {
	crate::{Db, Index, Key, Kind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

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

	pub(crate) fn expire_usage_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::expire::Arg,
		partition_total: u64,
	) -> tg::Result<tangram_index::usage::expire::Output> {
		if arg.partition_start > arg.partition_end || arg.partition_end > partition_total {
			return Err(tg::error!(
				"the usage expiration partition range is invalid"
			));
		}
		let mut keys = Vec::new();
		let mut pending = false;
		let mut unavailable = BTreeMap::new();
		Self::find_usage_delta_candidates(db, subspace, transaction, arg, &mut keys, &mut pending)?;
		if keys.len() < arg.batch_size {
			Self::find_usage_aggregate_candidates(
				db,
				subspace,
				transaction,
				arg,
				&mut keys,
				&mut pending,
				&mut unavailable,
			)?;
		}
		for ((account, kind, partition), through) in unavailable {
			Self::mark_usage_unavailable_with_transaction(
				db,
				subspace,
				transaction,
				&account,
				kind,
				partition,
				through,
			)?;
		}
		for key in &keys {
			db.delete(transaction, key)
				.map_err(|error| tg::error!(!error, "failed to delete usage data"))?;
		}
		let output = tangram_index::usage::expire::Output {
			deleted: keys.len(),
			done: keys.is_empty() && !pending,
		};

		Ok(output)
	}

	fn find_usage_delta_candidates(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
		arg: &tangram_index::usage::expire::Arg,
		keys: &mut Vec<Vec<u8>>,
		pending: &mut bool,
	) -> tg::Result<()> {
		let cutoff = arg
			.now
			.checked_sub(arg.delta_time_to_live)
			.unwrap_or(jiff::Timestamp::MIN);
		for partition in arg.partition_start..arg.partition_end {
			let prefix = Self::pack(subspace, &(Kind::UsageDelta.to_i32().unwrap(), partition));
			let entries = db
				.prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to iterate the usage deltas"))?;
			for entry in entries {
				if keys.len() == arg.batch_size {
					return Ok(());
				}
				let (key, _) =
					entry.map_err(|error| tg::error!(!error, "failed to read a usage delta"))?;
				let Key::Usage(crate::usage::Key::Delta {
					account,
					hour,
					partition,
					..
				}) = Self::unpack(subspace, key)?
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
				let aggregating = Self::contains_usage_aggregation_with_transaction(
					db,
					subspace,
					transaction,
					&account,
					hour,
					partition,
				)?;
				if aggregating {
					let current_hour = arg.now.as_second().div_euclid(60 * 60) * 60 * 60;
					*pending |= hour < current_hour;
				} else {
					keys.push(key.to_vec());
				}
			}
		}

		Ok(())
	}

	fn find_usage_aggregate_candidates(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
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
	) -> tg::Result<()> {
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
				let prefix = Self::pack(
					subspace,
					&(
						Kind::UsageAggregate.to_i32().unwrap(),
						partition,
						i32::from(kind as u8),
					),
				);
				let entries = db
					.prefix_iter(transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to iterate usage aggregates"))?;
				for entry in entries {
					if keys.len() == arg.batch_size {
						return Ok(());
					}
					let (key, _) = entry
						.map_err(|error| tg::error!(!error, "failed to read a usage aggregate"))?;
					let Key::Usage(crate::usage::Key::Aggregate {
						account,
						partition,
						period,
					}) = Self::unpack(subspace, key)?
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
							let next = Self::contains_usage_aggregation_with_transaction(
								db,
								subspace,
								transaction,
								&account,
								next_hour,
								partition,
							)?;
							let closing = Self::contains_usage_aggregation_with_transaction(
								db,
								subspace,
								transaction,
								&account,
								closing_hour,
								partition,
							)?;
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
								let contains = Self::contains_usage_aggregation_with_transaction(
									db,
									subspace,
									transaction,
									&account,
									closing_hour,
									partition,
								)?;
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
						keys.push(key.to_vec());
					}
				}
			}
		}

		Ok(())
	}
}
