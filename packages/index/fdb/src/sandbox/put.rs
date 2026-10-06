use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_sandboxes_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::sandbox::put::Arg],
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<tg::Result<()>, fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		let usage_partition_total = partition_totals.usage;
		for arg in args {
			if let Err(error) = arg.validate() {
				return Ok(ControlFlow::Break(Err(error)));
			}
			let key = Key::Sandbox(crate::sandbox::Key::Sandbox(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let existing = crate::retry!(result)
				.map(|bytes| tangram_index::sandbox::Sandbox::deserialize(&bytes))
				.transpose()?;

			// Validate sandbox data and processes against the existing record in this write transaction.
			if let Some(existing) = &existing {
				if let Err(error) = arg.validate_existing(existing)? {
					return Ok(ControlFlow::Break(Err(error)));
				}
				if !arg.principal.is_root()
					&& arg.principal != tg::Principal::Sandbox(arg.id.clone())
					&& let Some(processes) = &arg.processes
				{
					if !existing.set.processes {
						return Ok(ControlFlow::Break(Err(tg::error!(
							"cannot verify the existing sandbox processes"
						))));
					}
					let existing = crate::propagate!(
						Self::try_get_sandbox_processes_page_with_transaction(
							txn,
							subspace,
							&arg.id,
							std::io::SeekFrom::Start(0),
							processes.len() as u64 + 1
						)
						.await
					)
					.unwrap_or_default();
					if *processes != existing {
						return Ok(ControlFlow::Break(Err(tg::error!(
							"cannot replace the existing sandbox processes"
						))));
					}
				}
			}

			if existing.is_none() || (arg.data.is_some() && arg.processes.is_some()) {
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

			let mut arg = std::borrow::Cow::Borrowed(arg);
			if let Some(existing) = &existing
				&& !arg.principal.is_root()
				&& arg.principal != tg::Principal::Sandbox(arg.id.clone())
			{
				let arg = arg.to_mut();
				arg.account.clone_from(&existing.account);
				arg.data = None;
				arg.runner.clone_from(&existing.runner);
			}
			let arg = arg.as_ref();

			let processes_changed = arg.processes.is_some()
				&& existing
					.as_ref()
					.is_none_or(|existing| !existing.set.processes);

			// A delayed or replayed start must not overwrite a destroyed sandbox.
			if arg
				.data
				.as_ref()
				.is_some_and(|data| data.data.status.is_started())
				&& existing
					.as_ref()
					.and_then(|sandbox| sandbox.data.as_ref())
					.is_some_and(|data| data.data.status.is_destroyed())
			{
				continue;
			}
			let mut data = arg
				.data
				.clone()
				.or_else(|| existing.as_ref().and_then(|sandbox| sandbox.data.clone()));
			let location = arg.location.clone().or_else(|| {
				existing
					.as_ref()
					.and_then(|sandbox| sandbox.location.clone())
			});
			let account = arg.account.clone();
			if let Some(data) = &mut data {
				data.location.clone_from(&location);
			}
			let runner = arg
				.runner
				.clone()
				.or_else(|| existing.as_ref().and_then(|sandbox| sandbox.runner.clone()));
			let touched_at = existing.as_ref().map_or(arg.touched_at, |sandbox| {
				sandbox.touched_at.max(arg.touched_at)
			});
			let sandbox = tangram_index::sandbox::Sandbox {
				account,
				created_at: existing
					.as_ref()
					.map_or(arg.created_at, |sandbox| sandbox.created_at),
				data,
				location,
				reference_count: existing
					.as_ref()
					.map_or(0, |sandbox| sandbox.reference_count),
				runner,
				set: tangram_index::sandbox::Set {
					processes: arg.processes.is_some()
						|| existing
							.as_ref()
							.is_some_and(|sandbox| sandbox.set.processes),
				},
				touched_at,
			};
			if processes_changed && let Some(processes) = &arg.processes {
				crate::propagate!(
					Self::put_sandbox_processes_with_transaction(
						txn,
						subspace,
						&arg.id,
						processes,
						partition_total
					)
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
				let cpu = data.data.usage.as_ref().map(|usage| usage.cpu);
				let memory = data.data.usage.as_ref().map(|usage| usage.memory);
				let arg = tangram_index::usage::compute::put::Arg {
					account,
					at: touched_at,
					cpu,
					memory,
					sandbox_count: 1,
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

			if let Some(data) = existing
				.as_ref()
				.and_then(|sandbox| sandbox.data.as_ref())
				.filter(|data| data.data.status.is_started())
			{
				let creator = data.data.creator.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::sandbox::Key::CreatorSandbox {
					creator,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);

				let owner = data.data.owner.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::sandbox::Key::OwnerSandbox {
					owner,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}

			if let Some(data) = sandbox
				.data
				.as_ref()
				.filter(|data| data.data.status.is_started())
			{
				let creator = data.data.creator.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::sandbox::Key::CreatorSandbox {
					creator,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);

				let owner = data.data.owner.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::sandbox::Key::OwnerSandbox {
					owner,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);
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
					sandbox: arg.id.clone(),
					runner: runner.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}

			if sandbox
				.data
				.as_ref()
				.is_some_and(|data| data.data.status.is_started())
				&& let Some(runner) = &sandbox.runner
			{
				let key = Key::Runner(crate::runner::Key::RunnerSandbox {
					runner: runner.clone(),
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);

				let key = Key::Sandbox(crate::sandbox::Key::SandboxRunner {
					sandbox: arg.id.clone(),
					runner: runner.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);
			}
		}
		Ok(ControlFlow::Break(Ok(())))
	}
}
