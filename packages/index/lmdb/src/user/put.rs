use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_users_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::user::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::User(crate::user::Key::User(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let billing_ready = match arg.billing_ready {
				Some(billing_ready) => billing_ready,
				None => db
					.get(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get the user"))?
					.map_or(Ok(false), |bytes| {
						tangram_index::user::User::deserialize(bytes).map(|user| user.billing_ready)
					})?,
			};
			let value = tangram_index::user::User {
				billing_ready,
				specifier: arg.specifier.clone(),
			}
			.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the user"))?;

			let key = Key::Node(crate::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			db.put(transaction, &key, value.as_ref())
				.map_err(|error| tg::error!(!error, "failed to put the node"))?;
		}
		Ok(())
	}
}
