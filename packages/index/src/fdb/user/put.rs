#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
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

	pub(crate) async fn put_users_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::user::put::Arg],
	) -> crate::fdb::Result<()> {
		for arg in args {
			let key = Key::User(crate::fdb::user::Key::User(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let billing = match arg.billing {
				Some(billing) => billing,
				None => txn
					.get(&key, false)
					.await?
					.map_or(Ok(false), |bytes| {
						crate::user::User::deserialize(&bytes).map(|user| user.billing)
					})
					.map_err(crate::fdb::custom_error)?,
			};
			let value = crate::user::User {
				billing,
				specifier: arg.specifier.clone(),
			}
			.serialize()
			.map_err(crate::fdb::custom_error)?;
			txn.set(&key, &value);

			let key = Key::Node(crate::fdb::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			txn.set(&key, value.as_ref());
		}
		Ok(())
	}
}
