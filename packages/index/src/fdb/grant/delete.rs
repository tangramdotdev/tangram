use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
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
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(())
	}

	pub(crate) async fn task_delete_grants(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::grant::delete::Arg],
		partition_total: u64,
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Grant(crate::fdb::grant::Key::ResourceGrant {
				resource: arg.resource.clone(),
				principal: arg.principal.clone(),
				permission: arg.permission,
				expires_at: arg.expires_at,
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = Key::Grant(crate::fdb::grant::Key::PrincipalGrant {
				principal: arg.principal.clone(),
				resource: arg.resource.clone(),
				permission: arg.permission,
				expires_at: arg.expires_at,
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			for id in Self::ancestor_ids_with_transaction(txn, subspace, &arg.resource).await? {
				let key = Key::Grant(crate::fdb::grant::Key::Visibility {
					resource: id,
					principal: arg.principal.clone(),
					grant_resource: arg.resource.clone(),
					permission: arg.permission,
					expires_at: arg.expires_at,
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}
			if let Some(expires_at) = arg.expires_at {
				let partition = Self::partition_for_id(&arg.resource.to_bytes(), partition_total);
				let key = Key::Grant(crate::fdb::grant::Key::GrantExpiresAt {
					partition,
					expires_at,
					resource: arg.resource.clone(),
					principal: arg.principal.clone(),
					permission: arg.permission,
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}
		}
		Ok(())
	}
}
