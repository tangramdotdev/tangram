use {
	crate::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn touch_processes_with_account_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		arg: &crate::TouchProcesses,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<Vec<Option<tangram_index::process::Process>>, fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		let crate::TouchProcesses {
			account,
			ids,
			put_account,
			time_to_touch,
			touched_at,
		} = arg;
		let processes = crate::propagate!(
			Self::touch_processes_with_transaction(
				txn,
				subspace,
				ids,
				*touched_at,
				*time_to_touch,
				partition_total,
			)
			.await
		);
		if let Some(account) = account.as_ref() {
			{
				let result = future::try_join_all(
					std::iter::zip(ids, &processes)
						.filter(|(_, process)| process.is_some())
						.map(|(id, _)| {
							let arg = tangram_index::usage::storage::put::ProcessArg {
								account: account.clone(),
								process: id.clone(),
								touched_at: *touched_at,
							};
							async move {
								if *put_account
									&& crate::propagate!(
										Self::put_account_process(
											txn,
											subspace,
											&arg,
											partition_totals,
											Some(tg::authorization::permission::Set::Process(
												tg::authorization::permission::process::Set::all()
											)),
											None,
										)
										.await
									) {
									return Ok::<_, tg::Error>(ControlFlow::Break(()));
								}
								crate::propagate!(
									Self::touch_account_process(
										txn,
										subspace,
										&arg,
										*time_to_touch,
										partition_total,
									)
									.await
								);

								Ok::<_, tg::Error>(ControlFlow::Break(()))
							}
						}),
				)
				.await;
				let results = result?;
				for result in results {
					match result {
						ControlFlow::Break(()) => {},
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				}
			};
		}

		Ok(ControlFlow::Break(processes))
	}

	pub(crate) async fn touch_processes_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Vec<Option<tangram_index::process::Process>>, fdb::FdbError>> {
		let processes = {
			let result = future::try_join_all(ids.iter().map(|id| {
				let subspace = subspace.clone();
				async move {
					Self::touch_process_with_transaction(
						txn,
						&subspace,
						id,
						touched_at,
						time_to_touch,
						partition_total,
					)
					.await
				}
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

		Ok(ControlFlow::Break(processes))
	}

	async fn touch_process_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Option<tangram_index::process::Process>, fdb::FdbError>> {
		let key = Key::Process(crate::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let existing = crate::retry!(result);
		let existing = existing
			.as_ref()
			.map(|bytes| tangram_index::process::Process::deserialize(bytes))
			.transpose()?;
		let Some(mut process) = existing else {
			return Ok(ControlFlow::Break(None));
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - process.touched_at < time_to_touch {
			return Ok(ControlFlow::Break(Some(process)));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		let result = txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read);
		crate::retry!(result);

		process.touched_at = process.touched_at.max(touched_at);
		let value = process
			.serialize()
			.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;
		txn.set(&key, &value);
		if process.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = crate::Key::Clean(crate::clean::Key::Process {
				id: id.clone(),
				partition,
				touched_at: process.touched_at,
			});
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(ControlFlow::Break(Some(process)))
	}
}
