use {
	crate::lmdb::{Db, Index, Key, Request},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_grants(&self, args: &[crate::grant::delete::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteGrants(args.to_vec());
		self.sender_high
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_delete_grants(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::grant::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
				resource: arg.resource.clone(),
				principal: arg.principal.clone(),
				permission: arg.permission,
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the resource grant"))?;

			let key = Key::Grant(crate::lmdb::grant::Key::PrincipalGrant {
				principal: arg.principal.clone(),
				resource: arg.resource.clone(),
				permission: arg.permission,
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the principal grant"))?;

			let ids =
				Self::ancestor_ids_with_transaction(db, subspace, transaction, &arg.resource)?;
			for id in ids {
				let key = Key::Grant(crate::lmdb::grant::Key::Visibility {
					resource: id,
					principal: arg.principal.clone(),
					grant_resource: arg.resource.clone(),
					permission: arg.permission,
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the visibility entry"))?;
			}
		}
		Ok(())
	}
}
