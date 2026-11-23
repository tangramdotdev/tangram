use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::sandbox::put::Arg],
		process_data_fragment_size: usize,
		usage_partition_total: u64,
	) -> tg::Result<tg::Result<()>> {
		let mut rejection = None;
		for arg in args {
			let result = Self::put_sandbox_with_transaction(
				db,
				subspace,
				transaction,
				arg,
				usage_partition_total,
			)?;
			let zero_end = match result {
				Ok(zero_end) => zero_end,
				Err(error) => {
					rejection.get_or_insert(error);
					continue;
				},
			};

			// Fail the unfinished processes of a boot that this put ended with the number zero, in the same transaction.
			if let Some((boot, sandbox)) = zero_end {
				Self::fail_sandbox_boot_processes_with_transaction(
					db,
					subspace,
					transaction,
					&arg.id,
					boot,
					&sandbox,
					process_data_fragment_size,
					usage_partition_total,
				)?;
			}

			// Put the process that the sandbox carries. A rejected process leaves the sandbox in place, because the sandbox is valid without it.
			if let Some(process) = &arg.process {
				let result = Self::put_processes_with_transaction(
					db,
					subspace,
					transaction,
					std::slice::from_ref(process.as_ref()),
					process_data_fragment_size,
					usage_partition_total,
				)?;
				if let Err(error) = result {
					rejection.get_or_insert(error);
				}
			}
		}

		// Return the first rejection, since a rejected sandbox does not affect the other sandboxes.
		let result = rejection.map_or(Ok(()), Err);

		Ok(result)
	}

	pub(crate) fn put_sandbox_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::sandbox::put::Arg,
		usage_partition_total: u64,
	) -> tg::Result<tg::Result<Option<(u64, tangram_index::sandbox::Sandbox)>>> {
		let key = Key::Sandbox(crate::sandbox::Key::Sandbox(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let existing = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?
			.map(tangram_index::sandbox::Sandbox::deserialize)
			.transpose()?;

		// Get the existing processes when the merge compares them.
		let existing_processes = if arg.compares_processes(existing.as_ref()) {
			let length = arg.processes.as_ref().map_or(0, Vec::len) as u64 + 1;
			Self::try_get_sandbox_processes_page_with_transaction(
				db,
				subspace,
				transaction,
				&arg.id,
				std::io::SeekFrom::Start(0),
				length,
			)?
		} else {
			None
		};

		// Merge the put with the existing record in this write transaction.
		let merge = match arg.merge(existing.as_ref(), existing_processes.as_deref())? {
			Ok(merge) => merge,
			Err(error) => return Ok(Err(error)),
		};
		if merge.permissions {
			Self::put_permissions_with_transaction(db, subspace, transaction, &arg.permissions)?;
		}
		let sandbox = merge.sandbox;
		let zero_end = merge.zero_end.map(|boot| (boot, sandbox.clone()));
		let touched_at = sandbox.touched_at;
		if let Some(processes) = &arg.processes {
			Self::put_sandbox_processes_with_transaction(
				db,
				subspace,
				transaction,
				&arg.id,
				processes,
			)?;
		}
		let value = sandbox.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the sandbox"))?;

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
			Self::put_compute_usage(db, subspace, transaction, arg, usage_partition_total)?;
		}

		if let Some(existing) = &existing {
			let key = Key::Clean(crate::clean::Key::Sandbox {
				id: arg.id.clone(),
				touched_at: existing.touched_at,
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the clean key"))?;
		}

		if sandbox
			.data
			.as_ref()
			.is_some_and(|data| data.data.status.is_destroyed())
		{
			let key = Key::Clean(crate::clean::Key::Sandbox {
				id: arg.id.clone(),
				touched_at,
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;
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
					db.delete(transaction, &key).map_err(|error| {
						tg::error!(!error, "failed to delete a sandbox principal key")
					})?;
				}
			}
			if let Some((creator, owner, status)) = &current {
				for key in crate::sandbox::Key::principals(creator, owner, *status, &arg.id) {
					let key = Self::pack(subspace, &Key::Sandbox(key));
					db.put(transaction, &key, &[]).map_err(|error| {
						tg::error!(!error, "failed to put a sandbox principal key")
					})?;
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
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the runner sandbox"))?;

			let key = Key::Sandbox(crate::sandbox::Key::SandboxRunner {
				runner: runner.clone(),
				sandbox: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the sandbox runner"))?;
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
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the runner sandbox"))?;

			let key = Key::Sandbox(crate::sandbox::Key::SandboxRunner {
				runner: runner.clone(),
				sandbox: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the sandbox runner"))?;
		}
		Ok(Ok(zero_end))
	}

	/// Fail every unfinished process of a sandbox's boot, which the end of the boot by the sandbox's scheduler covers.
	#[expect(
		clippy::too_many_arguments,
		reason = "the failure of a boot's processes has this many inputs"
	)]
	fn fail_sandbox_boot_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::sandbox::Id,
		boot: u64,
		sandbox: &tangram_index::sandbox::Sandbox,
		process_data_fragment_size: usize,
		usage_partition_total: u64,
	) -> tg::Result<()> {
		// Get the unfinished processes of the boot.
		let id_bytes = id.to_bytes();
		let prefix = (
			Kind::SandboxBootProcess.to_i32().unwrap(),
			id_bytes.as_ref(),
			boot,
		);
		let prefix = Self::pack(subspace, &prefix);
		let processes = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, %id, "failed to get the processes of the boot"))?
			.map(|entry| {
				let (key, _) = entry.map_err(
					|error| tg::error!(!error, %id, "failed to read a process of the boot"),
				)?;
				let Key::Sandbox(crate::sandbox::Key::SandboxBootProcess { process, .. }) =
					Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(process)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		// Put each unfinished process as failed.
		let mut args = Vec::new();
		for process in processes {
			let existing =
				Self::try_get_process_with_transaction(db, subspace, transaction, &process)?;
			let data =
				Self::try_get_process_data_with_transaction(db, subspace, transaction, &process)?;
			args.extend(tangram_index::process::put::Arg::failed(
				&process,
				existing.as_ref(),
				data,
				sandbox,
			));
		}
		Self::put_processes_with_transaction(
			db,
			subspace,
			transaction,
			&args,
			process_data_fragment_size,
			usage_partition_total,
		)?
		.map_err(|error| tg::error!(!error, %id, "failed to fail the processes of the boot"))?;

		Ok(())
	}
}
