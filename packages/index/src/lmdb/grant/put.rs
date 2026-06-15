use {
	crate::lmdb::{Db, Index, Key, Request},
	foundationdb_tuple as fdbt, heed as lmdb,
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
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_put_grants(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::grant::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
				resource: arg.resource.clone(),
				principal: arg.principal.clone(),
				permission: arg.permission,
				expires_at: arg.expires_at,
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the resource grant"))?;

			let key = Key::Grant(crate::lmdb::grant::Key::PrincipalGrant {
				principal: arg.principal.clone(),
				resource: arg.resource.clone(),
				permission: arg.permission,
				expires_at: arg.expires_at,
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the principal grant"))?;

			let ids =
				Self::ancestor_ids_with_transaction(db, subspace, transaction, &arg.resource)?;
			for id in ids {
				let key = Key::Grant(crate::lmdb::grant::Key::Visibility {
					resource: id,
					principal: arg.principal.clone(),
					grant_resource: arg.resource.clone(),
					permission: arg.permission,
					expires_at: arg.expires_at,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the visibility entry"))?;
			}
			if let Some(expires_at) = arg.expires_at {
				let key = Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
					expires_at,
					resource: arg.resource.clone(),
					principal: arg.principal.clone(),
					permission: arg.permission,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the grant expiration"))?;
			}
		}
		Ok(())
	}
}
