use {
	crate::fdb::{Index, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{FutureExt as _, future::BoxFuture, future::try_join_all},
	std::ops::ControlFlow,
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

	pub(crate) async fn get_usage_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		account: &crate::usage::Account,
		period: crate::usage::Period,
		now: jiff::Timestamp,
		partition_total: u64,
	) -> tg::Result<ControlFlow<crate::usage::Aggregate, fdb::FdbError>> {
		let started = crate::fdb::propagate!(
			Self::try_get_usage_started_with_transaction(txn, subspace).await
		)
		.ok_or_else(|| tg::error!("usage tracking has not started"))?;
		if period.start().as_second() < started && period.end() <= now {
			return Err(tg::error!("usage is unavailable for the requested period"));
		}
		let cutoffs = {
			let result = try_join_all((0..partition_total).map(|partition| {
				Self::try_get_usage_unavailable_with_transaction(
					txn,
					subspace,
					account,
					period.kind(),
					partition,
				)
			}))
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};
		if cutoffs
			.into_iter()
			.flatten()
			.any(|cutoff| period.end().as_second() <= cutoff)
		{
			return Err(tg::error!("usage is unavailable for the requested period"));
		}
		if period.start() > now {
			return Ok(ControlFlow::Break(crate::usage::Aggregate::default()));
		}

		let mut aggregate = crate::usage::PartitionAggregate::default();
		let current_hour = now.as_second().div_euclid(60 * 60) * 60 * 60;
		let end_hour = period.end().as_second().min(current_hour);
		for partition in 0..partition_total {
			crate::fdb::propagate!(
				Self::aggregate_usage_compactions_for_account_with_transaction(
					txn, subspace, account, partition, end_hour, None,
				)
				.await
			);
			let value = crate::fdb::propagate!(
				Self::aggregate_usage_period_with_transaction(
					txn, subspace, account, partition, period, now,
				)
				.await
			);
			aggregate.checked_add(value)?;
		}

		let aggregate = aggregate.try_into_aggregate()?;

		Ok(ControlFlow::Break(aggregate))
	}

	fn aggregate_usage_period_with_transaction<'a>(
		txn: &'a crate::fdb::Transaction,
		subspace: &'a fdbt::Subspace,
		account: &'a crate::usage::Account,
		partition: u64,
		period: crate::usage::Period,
		now: jiff::Timestamp,
	) -> BoxFuture<'a, tg::Result<ControlFlow<crate::usage::PartitionAggregate, fdb::FdbError>>> {
		async move {
			if period.start() > now {
				return Ok(ControlFlow::Break(
					crate::usage::PartitionAggregate::default(),
				));
			}
			if period.end() <= now {
				let aggregate = crate::fdb::propagate!(
					Self::try_get_usage_aggregate_with_transaction(
						txn, subspace, account, partition, period,
					)
					.await
				);

				return Ok(ControlFlow::Break(aggregate.unwrap_or_default()));
			}

			let aggregate = match period {
				crate::usage::Period::Hour(_) => {
					crate::fdb::propagate!(
						Self::aggregate_usage_hour_with_transaction(
							txn,
							subspace,
							account,
							period.start().as_second(),
							partition,
							period.end() <= now,
						)
						.await
					)
				},
				crate::usage::Period::Day(_)
				| crate::usage::Period::Month(_)
				| crate::usage::Period::Week(_) => {
					let mut aggregate = crate::usage::PartitionAggregate::default();
					for child in crate::usage::children(period)? {
						if child.start() > now {
							break;
						}
						let child = crate::fdb::propagate!(
							Self::aggregate_usage_period_with_transaction(
								txn, subspace, account, partition, child, now,
							)
							.await
						);
						aggregate.checked_add(child)?;
					}
					if period.end() <= now {
						Self::put_usage_aggregate_with_transaction(
							txn, subspace, account, partition, period, aggregate,
						);
					}
					aggregate
				},
			};

			Ok(ControlFlow::Break(aggregate))
		}
		.boxed()
	}
}
