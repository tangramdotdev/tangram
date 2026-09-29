#![allow(clippy::unnecessary_wraps)]

use {
	crate::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_users(&self, args: &[tangram_index::user::put::Arg]) -> tg::Result<()> {
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
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::user::put::Arg],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			let key = Key::User(crate::user::Key::User(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let billing_ready = if let Some(billing_ready) = arg.billing_ready {
				billing_ready
			} else {
				let result = txn.get(&key, false).await;
				crate::retry!(result).map_or(Ok(false), |bytes| {
					tangram_index::user::User::deserialize(&bytes).map(|user| user.billing_ready)
				})?
			};
			let value = tangram_index::user::User {
				billing_ready,
				specifier: arg.specifier.clone(),
			}
			.serialize()?;
			txn.set(&key, &value);

			let key = Key::Node(crate::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			txn.set(&key, value.as_ref());
		}
		Ok(ControlFlow::Break(()))
	}
}
