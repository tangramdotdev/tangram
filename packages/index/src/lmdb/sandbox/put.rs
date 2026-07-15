use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn task_put_sandboxes(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::sandbox::put::Arg],
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
			let runner = arg
				.runner
				.clone()
				.or_else(|| existing.as_ref().and_then(|sandbox| sandbox.runner.clone()));
			let sandbox = crate::sandbox::Sandbox {
				created_at: existing
					.as_ref()
					.map_or(arg.created_at, |sandbox| sandbox.created_at),
				data,
				runner,
			};
			let value = sandbox.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the sandbox"))?;

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
