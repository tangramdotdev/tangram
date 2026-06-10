#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_grants(&self, args: &[crate::grant::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutGrants(args.to_vec());
		self.sender_medium
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(())
	}

	pub(crate) fn task_put_grants(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::grant::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Grant(crate::fdb::grant::Key::ResourceGrant {
				resource: arg.resource.clone(),
				principal: arg.principal.clone(),
				permission: arg.permission,
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Grant(crate::fdb::grant::Key::PrincipalGrant {
				principal: arg.principal.clone(),
				resource: arg.resource.clone(),
				permission: arg.permission,
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(())
	}
}
