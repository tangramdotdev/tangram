use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn delete_users_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::user::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::User(crate::user::Key::User(id.clone()));
			let key = Self::pack(subspace, &key);
			let user = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the user"))?
				.map(tangram_index::user::User::deserialize)
				.transpose()?;
			if let Some(user) = user {
				let key = Key::Node(crate::node::Key::Node(user.specifier));
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the node"))?;
			}
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the user"))?;
		}
		Ok(())
	}
}
