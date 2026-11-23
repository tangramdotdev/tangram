use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
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
			if let Err(error) = result {
				rejection.get_or_insert(error);
				continue;
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
	) -> tg::Result<tg::Result<()>> {
		if let Some(registration) = &arg.registration {
			let result = Self::check_runner_registration_with_transaction(
				db,
				subspace,
				transaction,
				registration,
			)?;
			if let Err(error) = result {
				return Ok(Err(error));
			}
		}
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
		Ok(Ok(()))
	}
}
