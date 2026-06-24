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
			let value = arg
				.owner
				.as_ref()
				.map(ToString::to_string)
				.unwrap_or_default();
			db.put(transaction, &key, value.as_bytes())
				.map_err(|error| tg::error!(!error, "failed to put the sandbox"))?;
		}
		Ok(())
	}
}
