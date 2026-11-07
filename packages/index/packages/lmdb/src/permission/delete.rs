use {
	crate::{
		Db, Index, Key, Kind,
		permission::{PermissionIndexEntry, PermissionSource, PermissionValue},
	},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn delete_subject_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		subject: &tg::authorization::Subject,
	) -> tg::Result<()> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SubjectPermission.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the subject permissions"))?
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to read a subject permission"))?;
				Ok((
					Self::unpack(subspace, key)?,
					PermissionValue::deserialize(value)?,
				))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for (key, value) in entries {
			let Key::Permission(crate::permission::Key::SubjectPermission { resource, subject }) =
				key
			else {
				return Err(tg::error!("expected a subject permission key"));
			};
			if matches!(subject, tg::authorization::Subject::Tag(_)) {
				if let Ok(object) = tg::object::Id::try_from(resource.clone()) {
					Self::schedule_object_accounts_for_cleaning(
						db,
						subspace,
						transaction,
						&object,
					)?;
				} else if let Ok(process) = tg::process::Id::try_from(resource.clone()) {
					Self::schedule_process_accounts_for_cleaning(
						db,
						subspace,
						transaction,
						&process,
					)?;
				}
			}
			for (permission, source, expires_at) in value.source_expirations() {
				let entry = PermissionIndexEntry {
					expires_at,
					permission,
					resource: &resource,
					subject: &subject,
				};
				Self::delete_permission_index_entry(db, subspace, transaction, &entry, source)?;
			}
			for permission in value.permissions.keys() {
				Self::enqueue_permission_update(
					db,
					subspace,
					transaction,
					&resource,
					&subject,
					*permission,
				)?;
			}
		}
		Ok(())
	}

	pub(crate) fn delete_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::permission::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			for permission in arg.permissions.iter() {
				let (expires_at, source) = match arg.source {
					tangram_index::permission::Source::Direct { expires_at } => {
						(expires_at, PermissionSource::Direct)
					},
					tangram_index::permission::Source::Grant => (None, PermissionSource::Grant),
				};
				let entry = PermissionIndexEntry {
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
			crate::permission::Key::ResourcePermission {
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
			},
		))
		.chain(std::iter::once(Key::Permission(
			crate::permission::Key::SubjectPermission {
				subject: entry.subject.clone(),
				resource: entry.resource.clone(),
			},
		)))
		.collect::<Vec<_>>();
		let mut empty = true;
		for key in keys {
			let key = Self::pack(subspace, &key);
			let Some(value) = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the permission entry"))?
			else {
				continue;
			};
			let mut value = PermissionValue::deserialize(value)?;
			let old_expires_at = value.source_expires_at(entry.permission, source).flatten();
			let deleted = value.delete(entry.permission, source, entry.expires_at);
			empty = empty && value.is_empty();
			if !deleted {
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

		// Delete the visibility entries of the ancestors if the subject has no remaining permissions on the resource.
		if empty {
			let ids =
				Self::ancestor_ids_with_transaction(db, subspace, transaction, entry.resource)?;
			for id in ids {
				let key = Key::Permission(crate::permission::Key::Visibility {
					resource: id,
					subject: entry.subject.clone(),
					permission_resource: entry.resource.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the visibility entry"))?;
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
