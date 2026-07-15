use {
	crate::fdb::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn task_put_sandboxes(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::sandbox::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?
				.map(|bytes| crate::sandbox::Sandbox::deserialize(&bytes))
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
			txn.set(&key, &value);

			if let Some(data) = existing
				.as_ref()
				.and_then(|sandbox| sandbox.data.as_ref())
				.filter(|data| data.status.is_started())
			{
				let creator = data.creator.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::fdb::sandbox::Key::CreatorSandbox {
					creator,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);

				let owner = data.owner.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::fdb::sandbox::Key::OwnerSandbox {
					owner,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}

			if let Some(data) = sandbox
				.data
				.as_ref()
				.filter(|data| data.status.is_started())
			{
				let creator = data.creator.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::fdb::sandbox::Key::CreatorSandbox {
					creator,
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);

				let owner = data.owner.clone().unwrap_or(tg::Principal::Root);
				let key = Key::Sandbox(crate::fdb::sandbox::Key::OwnerSandbox {
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
				let key = Key::Runner(crate::fdb::runner::Key::RunnerSandbox {
					runner: runner.clone(),
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);

				let key = Key::Sandbox(crate::fdb::sandbox::Key::SandboxRunner {
					sandbox: arg.id.clone(),
					runner: runner.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}

			if sandbox
				.data
				.as_ref()
				.is_some_and(|data| data.status.is_started())
				&& let Some(runner) = &sandbox.runner
			{
				let key = Key::Runner(crate::fdb::runner::Key::RunnerSandbox {
					runner: runner.clone(),
					sandbox: arg.id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);

				let key = Key::Sandbox(crate::fdb::sandbox::Key::SandboxRunner {
					sandbox: arg.id.clone(),
					runner: runner.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);
			}
		}
		Ok(())
	}
}
