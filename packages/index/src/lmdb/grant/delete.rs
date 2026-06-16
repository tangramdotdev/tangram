use {
	crate::lmdb::{
		Db, Index, Key, Request,
		grant::{GrantIndexEntry, GrantSource, grant_source_mask, grant_sources},
	},
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
			Self::delete_grant_index_entry(
				db,
				subspace,
				transaction,
				&GrantIndexEntry {
					expires_at: arg.expires_at,
					permission: arg.permission,
					principal: &arg.principal,
					resource: &arg.resource,
				},
				GrantSource::Explicit,
			)?;
			Self::enqueue_grant_update(
				db,
				subspace,
				transaction,
				&arg.resource,
				&arg.principal,
				arg.permission,
			)?;
		}
		Ok(())
	}

	pub(crate) fn delete_grant_index_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
	) -> tg::Result<bool> {
		let mask = grant_source_mask(source);
		let mut changed = false;
		let keys = std::iter::once(Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
			resource: entry.resource.clone(),
			principal: entry.principal.clone(),
			permission: entry.permission,
			expires_at: entry.expires_at,
		}))
		.chain(std::iter::once(Key::Grant(
			crate::lmdb::grant::Key::PrincipalGrant {
				principal: entry.principal.clone(),
				resource: entry.resource.clone(),
				permission: entry.permission,
				expires_at: entry.expires_at,
			},
		)))
		.collect::<Vec<_>>();
		for key in keys {
			let key = Self::pack(subspace, &key);
			let Some(value) = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the grant entry"))?
			else {
				continue;
			};
			let sources = grant_sources(value);
			let new_sources = sources & !mask;
			if new_sources == sources {
				continue;
			}
			if new_sources == 0 {
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the grant entry"))?;
			} else {
				db.put(transaction, &key, &[new_sources])
					.map_err(|error| tg::error!(!error, "failed to put the grant entry"))?;
			}
			changed = true;
		}

		let ids = Self::ancestor_ids_with_transaction(db, subspace, transaction, entry.resource)?;
		for id in ids {
			let key = Key::Grant(crate::lmdb::grant::Key::Visibility {
				resource: id,
				principal: entry.principal.clone(),
				grant_resource: entry.resource.clone(),
				permission: entry.permission,
				expires_at: entry.expires_at,
			});
			let key = Self::pack(subspace, &key);
			let Some(value) = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the visibility entry"))?
			else {
				continue;
			};
			let sources = grant_sources(value);
			let new_sources = sources & !mask;
			if new_sources == 0 {
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the visibility entry"))?;
			} else if new_sources != sources {
				db.put(transaction, &key, &[new_sources])
					.map_err(|error| tg::error!(!error, "failed to put the visibility entry"))?;
			}
		}
		if let Some(expires_at) = entry.expires_at {
			let key = Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
				expires_at,
				resource: entry.resource.clone(),
				principal: entry.principal.clone(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			if let Some(value) = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the grant expiration"))?
			{
				let sources = grant_sources(value);
				let new_sources = sources & !mask;
				if new_sources == 0 {
					db.delete(transaction, &key).map_err(|error| {
						tg::error!(!error, "failed to delete the grant expiration")
					})?;
				} else if new_sources != sources {
					db.put(transaction, &key, &[new_sources]).map_err(|error| {
						tg::error!(!error, "failed to put the grant expiration")
					})?;
				}
			}
		}
		Ok(changed)
	}
}
