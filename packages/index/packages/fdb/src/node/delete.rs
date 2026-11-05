use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn delete_node_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Id,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Node(crate::node::Key::Node(specifier.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);
		let id = id.to_bytes();
		if value.as_deref() == Some(id.as_ref()) {
			txn.clear(&key);
		}

		Ok(ControlFlow::Break(()))
	}
}
