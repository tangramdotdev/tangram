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
			let value = arg
				.owner
				.as_ref()
				.map(ToString::to_string)
				.unwrap_or_default();
			txn.set(&key, value.as_bytes());
		}
		Ok(())
	}
}
