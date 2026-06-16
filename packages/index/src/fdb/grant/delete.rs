use {
	crate::fdb::{
		Index, Key, Request, Response,
		grant::{GrantIndexEntry, GrantSource, grant_source_mask, grant_sources},
	},
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
			for permission in arg.permissions.iter() {
				Self::delete_grant_index_entry(
					txn,
					subspace,
					&GrantIndexEntry {
						creator: arg.creator.as_ref(),
						expires_at: arg.expires_at,
						permission,
						principal: &arg.principal,
						resource: &arg.resource,
					},
					GrantSource::Explicit,
					partition_total,
				)
				.await?;
				Self::enqueue_grant_update(
					txn,
					subspace,
					&arg.resource,
					&arg.principal,
					permission,
					partition_total,
				);
			}
		}
		Ok(())
	}

	pub(crate) async fn delete_grant_index_entry(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
		partition_total: u64,
	) -> tg::Result<bool> {
		let mask = grant_source_mask(source);
		let mut changed = false;
		let keys = std::iter::once(Key::Grant(crate::fdb::grant::Key::ResourceGrant {
			resource: entry.resource.clone(),
			principal: entry.principal.clone(),
			creator: entry.creator.cloned(),
			permission: entry.permission,
			expires_at: entry.expires_at,
		}))
		.chain(std::iter::once(Key::Grant(
			crate::fdb::grant::Key::PrincipalGrant {
				principal: entry.principal.clone(),
				resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				expires_at: entry.expires_at,
			},
		)))
		.collect::<Vec<_>>();
		for key in keys {
			let key = Self::pack(subspace, &key);
			let Some(value) = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the grant entry"))?
			else {
				continue;
			};
			let sources = grant_sources(&value);
			let new_sources = sources & !mask;
			if new_sources == sources {
				continue;
			}
			if new_sources == 0 {
				txn.clear(&key);
			} else {
				txn.set(&key, &[new_sources]);
			}
			changed = true;
		}

		for id in Self::ancestor_ids_with_transaction(txn, subspace, entry.resource).await? {
			let key = Key::Grant(crate::fdb::grant::Key::Visibility {
				resource: id,
				principal: entry.principal.clone(),
				grant_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				expires_at: entry.expires_at,
			});
			let key = Self::pack(subspace, &key);
			let Some(value) = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the visibility entry"))?
			else {
				continue;
			};
			let sources = grant_sources(&value);
			let new_sources = sources & !mask;
			if new_sources == 0 {
				txn.clear(&key);
			} else if new_sources != sources {
				txn.set(&key, &[new_sources]);
			}
		}
		if let Some(expires_at) = entry.expires_at {
			let partition = Self::partition_for_id(&entry.resource.to_bytes(), partition_total);
			let key = Key::Grant(crate::fdb::grant::Key::GrantExpiresAt {
				partition,
				expires_at,
				resource: entry.resource.clone(),
				principal: entry.principal.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			if let Some(value) = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the grant expiration"))?
			{
				let sources = grant_sources(&value);
				let new_sources = sources & !mask;
				if new_sources == 0 {
					txn.clear(&key);
				} else if new_sources != sources {
					txn.set(&key, &[new_sources]);
				}
			}
		}
		Ok(changed)
	}
}
