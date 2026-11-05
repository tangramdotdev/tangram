use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn delete_node_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Id,
		specifier: &tg::Specifier,
	) -> tg::Result<()> {
		let key = Key::Node(crate::node::Key::Node(specifier.clone()));
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the node"))?;
		let id = id.to_bytes();
		if value == Some(id.as_ref()) {
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the node"))?;
		}

		Ok(())
	}
}
