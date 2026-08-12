use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_sandboxes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::sandbox::put::Arg],
		usage_partition_total: u64,
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?
				.map(crate::sandbox::Sandbox::deserialize)
				.transpose()?;
			let data = arg
				.data
				.clone()
				.or_else(|| existing.as_ref().and_then(|sandbox| sandbox.data.clone()));
			let account = arg.account.clone();
			let runner = arg
				.runner
				.clone()
				.or_else(|| existing.as_ref().and_then(|sandbox| sandbox.runner.clone()));
			let touched_at = existing.as_ref().map_or(arg.touched_at, |sandbox| {
				sandbox.touched_at.max(arg.touched_at)
			});
			let sandbox = crate::sandbox::Sandbox {
				account,
				created_at: existing
					.as_ref()
					.map_or(arg.created_at, |sandbox| sandbox.created_at),
				data,
				reference_count: existing
					.as_ref()
					.map_or(0, |sandbox| sandbox.reference_count),
				runner,
				touched_at,
			};
			let value = sandbox.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the sandbox"))?;

			let started = existing
				.as_ref()
				.and_then(|sandbox| sandbox.data.as_ref())
				.is_some_and(|data| data.status.is_started());
			if started
				&& let (Some(account), Some(data)) = (&sandbox.account, &sandbox.data)
				&& data.status.is_destroyed()
			{
				let cpu = data.usage.as_ref().map(|usage| usage.cpu);
				let memory = data.usage.as_ref().map(|usage| usage.memory);
				let arg = crate::usage::compute::put::Arg {
					account,
					at: touched_at,
					cpu,
					memory,
					sandbox_count: 1,
				};
				Self::put_compute_usage(db, subspace, transaction, arg, usage_partition_total)?;
			}

			if let Some(existing) = &existing {
				let key = Key::Clean(crate::lmdb::clean::Key::Sandbox {
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
				.is_some_and(|data| data.status.is_destroyed())
			{
				let key = Key::Clean(crate::lmdb::clean::Key::Sandbox {
					id: arg.id.clone(),
					touched_at,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;
			}

			if let Some(data) = existing
				.as_ref()
				.and_then(|sandbox| sandbox.data.as_ref())
				.filter(|data| data.status.is_started())
			{
				let creator = data.creator.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::lmdb::sandbox::Key::CreatorSandbox {
					creator,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the creator sandbox"))?;

				let owner = data.owner.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::lmdb::sandbox::Key::OwnerSandbox {
					owner,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the owner sandbox"))?;
			}

			if let Some(data) = sandbox
				.data
				.as_ref()
				.filter(|data| data.status.is_started())
			{
				let creator = data.creator.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::lmdb::sandbox::Key::CreatorSandbox {
					creator,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the creator sandbox"))?;

				let owner = data.owner.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::lmdb::sandbox::Key::OwnerSandbox {
					owner,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the owner sandbox"))?;
			}

			if let Some(runner) = existing
				.as_ref()
				.and_then(|sandbox| sandbox.runner.as_ref())
			{
				let key = Key::Runner(crate::lmdb::runner::Key::RunnerSandbox {
					runner: runner.clone(),
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the runner sandbox"))?;

				let key = Key::Sandbox(crate::lmdb::sandbox::Key::SandboxRunner {
					sandbox: arg.id.clone(),
					runner: runner.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the sandbox runner"))?;
			}

			if sandbox
				.data
				.as_ref()
				.is_some_and(|data| data.status.is_started())
				&& let Some(runner) = &sandbox.runner
			{
				let key = Key::Runner(crate::lmdb::runner::Key::RunnerSandbox {
					runner: runner.clone(),
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the runner sandbox"))?;

				let key = Key::Sandbox(crate::lmdb::sandbox::Key::SandboxRunner {
					sandbox: arg.id.clone(),
					runner: runner.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the sandbox runner"))?;
			}
		}
		Ok(())
	}
}
