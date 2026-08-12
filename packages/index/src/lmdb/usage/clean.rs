use {
	crate::lmdb::{Db, Index, Key, Kind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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

	pub(crate) fn clean_usage_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::usage::clean::Arg,
		partition_total: u64,
	) -> tg::Result<crate::usage::clean::Output> {
		let mut keys = Vec::new();
		let mut pending = false;
		Self::find_usage_delta_candidates(
			db,
			subspace,
			transaction,
			arg,
			partition_total,
			&mut keys,
			&mut pending,
		)?;
		if keys.len() < arg.batch_size {
			Self::find_usage_aggregate_candidates(
				db,
				subspace,
				transaction,
				arg,
				partition_total,
				&mut keys,
				&mut pending,
			)?;
		}
		for key in &keys {
			db.delete(transaction, key)
				.map_err(|error| tg::error!(!error, "failed to delete usage data"))?;
		}
		let output = crate::usage::clean::Output {
			deleted: keys.len(),
			done: keys.is_empty() && !pending,
		};

		Ok(output)
	}

	fn find_usage_delta_candidates(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RwTxn<'_>,
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
				let Key::Usage(crate::lmdb::usage::Key::Delta {
					account,
					hour,
					partition,
					..
				}) = Self::unpack(subspace, key)?
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
					db,
					subspace,
					transaction,
					&account,
					hour,
					partition,
				)?;
				if compacting {
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
					let Key::Usage(crate::lmdb::usage::Key::Aggregate {
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
								db,
								subspace,
								transaction,
								&account,
								next_hour,
								partition,
							)?;
							let closing = Self::contains_usage_compaction_with_transaction(
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
						crate::usage::Period::Month(_) | crate::usage::Period::Week(_) => {
							(false, false)
						},
					};
					if dependency {
						*pending |= eligible;
					} else {
						keys.push(key.to_vec());
					}
				}
			}
		}

		Ok(())
	}
}
