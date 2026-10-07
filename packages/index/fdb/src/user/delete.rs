#![allow(clippy::unnecessary_wraps)]

use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn delete_users_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::user::Id],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for id in ids {
			let key = Key::User(crate::user::Key::User(id.clone()));
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let user = crate::retry!(result)
				.map(|bytes| tangram_index::user::User::deserialize(&bytes))
				.transpose()?;
			if let Some(user) = user {
				let node_key = Key::Node(crate::node::Key::Node(user.specifier));
				let node_key = Self::pack(subspace, &node_key);
				txn.clear(&node_key);
			}
			txn.clear(&key);
		}
		Ok(ControlFlow::Break(()))
	}
}
