use {
	crate::lmdb::{
		Db, Index, Key, Request, Response,
		permission::{PermissionIndexEntry, PermissionSource, PermissionValue},
	},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_permissions(
		&self,
		args: &[crate::permission::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::DeletePermissions(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub(crate) fn delete_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::permission::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			for permission in arg.permissions.iter() {
				let (expires_at, source) = match arg.source {
					crate::permission::Source::Direct { expires_at } => {
						(expires_at, PermissionSource::Direct)
					},
					crate::permission::Source::Grant => (None, PermissionSource::Grant),
				};
				let entry = PermissionIndexEntry {
					creator: arg.creator.as_ref(),
					expires_at,
					permission,
					subject: &arg.subject,
					resource: &arg.resource,
				};
				let changed =
					Self::delete_permission_index_entry(db, subspace, transaction, &entry, source)?;
				if changed {
					Self::enqueue_permission_update(
						db,
						subspace,
						transaction,
						&arg.resource,
						&arg.subject,
						permission,
					)?;
				}
			}
		}
		Ok(())
	}

	pub(crate) fn delete_permission_index_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &PermissionIndexEntry<'_>,
		source: PermissionSource,
	) -> tg::Result<bool> {
		let mut changed = false;
		let keys = std::iter::once(Key::Permission(
			crate::lmdb::permission::Key::ResourcePermission {
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			},
		))
		.chain(std::iter::once(Key::Permission(
			crate::lmdb::permission::Key::SubjectPermission {
				subject: entry.subject.clone(),
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
				.map_err(|error| tg::error!(!error, "failed to get the permission entry"))?
			else {
				continue;
			};
			let mut value = PermissionValue::deserialize(value)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if !value.delete(source, entry.expires_at) {
				continue;
			}
			if value.is_empty() {
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the permission entry"))?;
			} else {
				let bytes = value.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put the permission entry"))?;
			}
			Self::update_permission_expiration(
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
			let key = Key::Permission(crate::lmdb::permission::Key::Visibility {
				resource: id,
				subject: entry.subject.clone(),
				permission_resource: entry.resource.clone(),
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
			let mut value = PermissionValue::deserialize(value)?;
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
		Self::update_permission_expiration(
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
