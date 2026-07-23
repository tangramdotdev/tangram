use {
	crate::lmdb::{Db, Index, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_users(&self, args: &[crate::user::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutUsers(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub(crate) fn put_users_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::user::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::User(crate::lmdb::user::Key::User(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let value = crate::user::User {
				specifier: arg.specifier.clone(),
			}
			.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the user"))?;

			let key = Key::Node(crate::lmdb::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			db.put(transaction, &key, value.as_ref())
				.map_err(|error| tg::error!(!error, "failed to put the node"))?;
		}
		Ok(())
	}
}
