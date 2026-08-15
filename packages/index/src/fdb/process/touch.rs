use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.touch_processes_inner(ids, None, false, touched_at, time_to_touch)
			.await
	}

	pub async fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &crate::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.touch_processes_inner(ids, Some(account), true, touched_at, time_to_touch)
			.await
	}

	pub async fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.touch_processes_inner(ids, account, false, touched_at, time_to_touch)
			.await
	}

	async fn touch_processes_inner(
		&self,
		ids: &[tg::process::Id],
		account: Option<&crate::usage::Account>,
		put_account: bool,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchProcesses(crate::fdb::TouchProcesses {
			account: account.cloned(),
			ids: ids.to_vec(),
			put_account,
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::Processes(processes) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(processes)
	}

	pub(in crate::fdb) async fn touch_processes_with_account_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &crate::fdb::TouchProcesses,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<ControlFlow<Vec<Option<crate::process::Process>>, fdb::FdbError>> {
		let crate::fdb::TouchProcesses {
			account,
			ids,
			put_account,
			time_to_touch,
			touched_at,
		} = arg;
		let processes = crate::fdb::propagate!(
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
							let arg = crate::usage::storage::put::ProcessArg {
								account: account.clone(),
								process: id.clone(),
								touched_at: *touched_at,
							};
							async move {
								if *put_account
									&& crate::fdb::propagate!(
										Self::put_account_process(
											txn,
											subspace,
											&arg,
											partition_total,
											usage_partition_total,
											false,
											None,
										)
										.await
									) {
									return Ok::<_, tg::Error>(ControlFlow::Break(()));
								}
								crate::fdb::propagate!(
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
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Vec<Option<crate::process::Process>>, fdb::FdbError>> {
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
			let mut values = Vec::new();
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
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Option<crate::process::Process>, fdb::FdbError>> {
		let key = Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let existing = crate::fdb::retry!(txn.get(&key, false).await);
		let existing = existing
			.as_ref()
			.map(|bytes| crate::process::Process::deserialize(bytes))
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
		crate::fdb::retry!(txn.add_conflict_range(
			&key,
			&key_end,
			fdb::options::ConflictRangeType::Read,
		));

		process.touched_at = process.touched_at.max(touched_at);
		let value = process
			.serialize()
			.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;
		txn.set(&key, &value);
		if process.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Process {
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
