use {
	crate::lmdb::{
		Db, Index, Key, Request, Response,
		grant::{GrantIndexEntry, GrantSource, GrantValue},
	},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_grants(&self, args: &[crate::grant::delete::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteGrants(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub(crate) fn delete_grants_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::grant::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			for permission in arg.permissions.iter() {
				let source = if arg.expires_at.is_some() {
					GrantSource::Temporary
				} else {
					GrantSource::Explicit
				};
				let changed = Self::delete_grant_index_entry(
					db,
					subspace,
					transaction,
					&GrantIndexEntry {
						creator: arg.creator.as_ref(),
						expires_at: arg.expires_at,
						permission,
						principal: &arg.principal,
						resource: &arg.resource,
					},
					source,
				)?;
				if changed {
					Self::enqueue_grant_update(
						db,
						subspace,
						transaction,
						&arg.resource,
						&arg.principal,
						permission,
					)?;
				}
			}
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
		let mut changed = false;
		let keys = std::iter::once(Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
			resource: entry.resource.clone(),
			principal: entry.principal.clone(),
			creator: entry.creator.cloned(),
			permission: entry.permission,
		}))
		.chain(std::iter::once(Key::Grant(
			crate::lmdb::grant::Key::PrincipalGrant {
				principal: entry.principal.clone(),
				resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
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
			let mut value = GrantValue::deserialize(value)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if !value.delete(source, entry.expires_at) {
				continue;
			}
			if value.is_empty() {
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the grant entry"))?;
			} else {
				let bytes = value.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put the grant entry"))?;
			}
			Self::update_grant_expiration(
				db,
				subspace,
				transaction,
				entry,
				source,
				old_expires_at,
				None,
			)?;
			changed = true;
		}

		let ids = Self::ancestor_ids_with_transaction(db, subspace, transaction, entry.resource)?;
		for id in ids {
			let key = Key::Grant(crate::lmdb::grant::Key::Visibility {
				resource: id,
				principal: entry.principal.clone(),
				grant_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let Some(value) = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the visibility entry"))?
			else {
				continue;
			};
			let mut value = GrantValue::deserialize(value)?;
			if !value.delete(source, entry.expires_at) {
				continue;
			}
			if value.is_empty() {
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the visibility entry"))?;
			} else {
				let bytes = value.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put the visibility entry"))?;
			}
		}
		Self::update_grant_expiration(
			db,
			subspace,
			transaction,
			entry,
			source,
			entry.expires_at,
			None,
		)?;
		Ok(changed)
	}
}
