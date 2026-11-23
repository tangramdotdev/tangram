use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_sandboxes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::sandbox::put::Arg],
		partition_totals: crate::PartitionTotals,
		process_data_fragment_size: usize,
	) -> tg::Result<ControlFlow<tg::Result<()>, fdb::FdbError>> {
		let mut rejection = None;
		for arg in args {
			let result = crate::propagate!(
				Self::put_sandbox_with_transaction(txn, subspace, arg, partition_totals).await
			);
			let zero_end = match result {
				Ok(zero_end) => zero_end,
				Err(error) => {
					rejection.get_or_insert(error);
					continue;
				},
			};

			// Fail the unfinished processes of a boot that this put ended with the number zero, in the same transaction.
			if let Some((boot, sandbox)) = zero_end {
				crate::propagate!(
					Self::fail_sandbox_boot_processes_with_transaction(
						txn,
						subspace,
						&arg.id,
						boot,
						&sandbox,
						partition_totals,
						process_data_fragment_size,
					)
					.await
				);
			}

			// Put the process that the sandbox carries. A rejected process leaves the sandbox in place, because the sandbox is valid without it.
			if let Some(process) = &arg.process {
				let result = crate::propagate!(
					Self::put_processes_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(process.as_ref()),
						partition_totals,
						process_data_fragment_size,
					)
					.await
				);
				if let Err(error) = result {
					rejection.get_or_insert(error);
				}
			}
		}

		// Return the first rejection, since a rejected sandbox does not affect the other sandboxes.
		let result = rejection.map_or(Ok(()), Err);

		Ok(ControlFlow::Break(result))
	}

	pub(crate) async fn put_sandbox_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::sandbox::put::Arg,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<
		ControlFlow<tg::Result<Option<(u64, tangram_index::sandbox::Sandbox)>>, fdb::FdbError>,
	> {
		let partition_total = partition_totals.cleaning;
		let usage_partition_total = partition_totals.usage;
		let key = Key::Sandbox(crate::sandbox::Key::Sandbox(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let existing = crate::retry!(result)
			.map(|bytes| tangram_index::sandbox::Sandbox::deserialize(&bytes))
			.transpose()?;

		// Get the existing processes when the merge compares them.
		let existing_processes = if arg.compares_processes(existing.as_ref()) {
			let length = arg.processes.as_ref().map_or(0, Vec::len) as u64 + 1;
			crate::propagate!(
				Self::try_get_sandbox_processes_page_with_transaction(
					txn,
					subspace,
					&arg.id,
					std::io::SeekFrom::Start(0),
					length
				)
				.await
			)
		} else {
			None
		};

		// Merge the put with the existing record in this write transaction.
		let merge = match arg.merge(existing.as_ref(), existing_processes.as_deref())? {
			Ok(merge) => merge,
			Err(error) => return Ok(ControlFlow::Break(Err(error))),
		};

		// Put the permissions if the merge grants them.
		if merge.permissions {
			crate::propagate!(
				Self::put_permissions_with_transaction(
					txn,
					subspace,
					&arg.permissions,
					partition_totals
				)
				.await
			);
		}

		// Store the record that the merge produces, with its processes.
		let sandbox = merge.sandbox;
		let zero_end = merge.zero_end.map(|boot| (boot, sandbox.clone()));
		let touched_at = sandbox.touched_at;
		if let Some(processes) = &arg.processes {
			crate::propagate!(
				Self::put_sandbox_processes_with_transaction(txn, subspace, &arg.id, processes)
					.await
			);
		}
		let value = sandbox.serialize()?;
		txn.set(&key, &value);

		let destroyed = existing
			.as_ref()
			.and_then(|sandbox| sandbox.data.as_ref())
			.is_some_and(|data| data.data.status.is_destroyed());
		if !destroyed
			&& let (Some(account), Some(data)) = (&sandbox.account, &sandbox.data)
			&& data.data.status.is_destroyed()
		{
			let arg = tangram_index::usage::compute::put::Arg {
				account,
				at: touched_at,
				sandbox_count: 1,
				usage: data.data.usage.as_ref(),
			};
			crate::propagate!(Self::put_compute_usage(
				txn,
				subspace,
				arg,
				usage_partition_total,
			));
		}

		let id_bytes = arg.id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		if let Some(existing) = &existing {
			let key = Key::Clean(crate::clean::Key::Sandbox {
				id: arg.id.clone(),
				partition,
				touched_at: existing.touched_at,
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}

		if sandbox
			.data
			.as_ref()
			.is_some_and(|data| data.data.status.is_destroyed())
		{
			let key = Key::Clean(crate::clean::Key::Sandbox {
				id: arg.id.clone(),
				partition,
				touched_at,
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		// Move the creator and owner keys when the creator, owner, or status changes.
		let previous = existing
			.as_ref()
			.and_then(|sandbox| sandbox.data.as_ref())
			.map(|data| {
				(
					data.data.creator.clone(),
					data.data.owner.clone(),
					data.data.status,
				)
			});
		let current = sandbox.data.as_ref().map(|data| {
			(
				data.data.creator.clone(),
				data.data.owner.clone(),
				data.data.status,
			)
		});
		if previous != current {
			if let Some((creator, owner, status)) = &previous {
				for key in crate::sandbox::Key::principals(creator, owner, *status, &arg.id) {
					let key = Self::pack(subspace, &Key::Sandbox(key));
					txn.clear(&key);
				}
			}
			if let Some((creator, owner, status)) = &current {
				for key in crate::sandbox::Key::principals(creator, owner, *status, &arg.id) {
					let key = Self::pack(subspace, &Key::Sandbox(key));
					txn.set(&key, &[]);
				}
			}
		}

		if let Some(runner) = existing
			.as_ref()
			.and_then(|sandbox| sandbox.runner.as_ref())
		{
			let key = Key::Runner(crate::runner::Key::RunnerSandbox {
				runner: runner.clone(),
				sandbox: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = Key::Sandbox(crate::sandbox::Key::SandboxRunner {
				runner: runner.clone(),
				sandbox: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}

		if sandbox
			.data
			.as_ref()
			.is_some_and(|data| !data.data.status.is_destroyed())
			&& let Some(runner) = &sandbox.runner
		{
			let key = Key::Runner(crate::runner::Key::RunnerSandbox {
				runner: runner.clone(),
				sandbox: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Sandbox(crate::sandbox::Key::SandboxRunner {
				runner: runner.clone(),
				sandbox: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(ControlFlow::Break(Ok(zero_end)))
	}

	/// Fail every unfinished process of a sandbox's boot, which the end of the boot by the sandbox's scheduler covers.
	async fn fail_sandbox_boot_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::sandbox::Id,
		boot: u64,
		sandbox: &tangram_index::sandbox::Sandbox,
		partition_totals: crate::PartitionTotals,
		process_data_fragment_size: usize,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Get the unfinished processes of the boot.
		let id_bytes = id.to_bytes();
		let prefix = (
			Kind::SandboxBootProcess.to_i32().unwrap(),
			id_bytes.as_ref(),
			boot,
		);
		let prefix = Self::pack(subspace, &prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let processes = entries
			.iter()
			.map(|entry| {
				let Key::Sandbox(crate::sandbox::Key::SandboxBootProcess { process, .. }) =
					Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(process)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		drop(entries);

		// Put each unfinished process as failed.
		let mut args = Vec::new();
		for process in processes {
			let existing = crate::propagate!(
				Self::try_get_process_with_transaction(txn, subspace, &process).await
			);
			let data = crate::propagate!(
				Self::try_get_process_data_with_transaction(txn, subspace, &process).await
			);
			args.extend(tangram_index::process::put::Arg::failed(
				&process,
				existing.as_ref(),
				data,
				sandbox,
			));
		}
		let result = crate::propagate!(
			Self::put_processes_with_transaction(
				txn,
				subspace,
				&args,
				partition_totals,
				process_data_fragment_size,
			)
			.await
		);
		result
			.map_err(|error| tg::error!(!error, %id, "failed to fail the processes of the boot"))?;

		Ok(ControlFlow::Break(()))
	}
}
