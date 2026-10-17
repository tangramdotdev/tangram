use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn get_resource_permission_subjects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
	) -> tg::Result<BTreeSet<tg::authorization::Subject>> {
		let bytes = resource.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ResourcePermission.to_i32().unwrap(), bytes.as_ref()),
		);
		let mut subjects = BTreeSet::new();
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the resource permissions"))?;
		for entry in entries {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read the resource permission"))?;
			let Key::Permission(crate::permission::Key::ResourcePermission { subject, .. }) =
				Self::unpack(subspace, key)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			subjects.insert(subject);
		}
		Ok(subjects)
	}

	pub(crate) fn get_resource_permission_entries_for_subject_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<Vec<crate::permission::PermissionEntry>> {
		let resource_bytes = resource.to_bytes();
		let prefix = &(
			Kind::ResourcePermission.to_i32().unwrap(),
			resource_bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut permissions = Vec::new();
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the resource permissions"))?;
		for entry in iter {
			let (key, value) = entry.map_err(|error| {
				tg::error!(!error, "failed to read the resource permission entry")
			})?;
			let key = Self::unpack(subspace, key)?;
			let Key::Permission(crate::permission::Key::ResourcePermission {
				creator,
				permission,
				subject,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = crate::permission::PermissionValue::deserialize(value)?;
			permissions.push(crate::permission::PermissionEntry {
				creator,
				grant: value.grant,
				direct: value.direct,
				materialized: value.materialized,
				permission,
				subject,
			});
		}
		Ok(permissions)
	}

	pub(crate) fn get_resource_sync_permission_entries_for_subject_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<Vec<crate::permission::PermissionEntry>> {
		let read = tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		);
		let subjects = Self::get_resource_permission_subjects_with_transaction(
			db,
			subspace,
			transaction,
			resource,
		)?;
		let mut permissions = Vec::new();
		for sync_subject in subjects {
			let tg::authorization::Subject::Sync(sync) = &sync_subject else {
				continue;
			};
			let sync_resource = tg::Id::from(sync.clone());
			let read_entries = Self::get_resource_permission_entries_for_subject_with_transaction(
				db,
				subspace,
				transaction,
				&sync_resource,
				subject,
			)?;
			let Some(read_expires_at) = read_entries
				.iter()
				.filter(|entry| entry.permission == read)
				.filter_map(|entry| entry.effective_expires_at())
				.reduce(crate::permission::max_expires_at)
			else {
				continue;
			};
			let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
				db,
				subspace,
				transaction,
				resource,
				&sync_subject,
			)?;
			for entry in entries {
				if !entry.permission.is_read_like() {
					continue;
				}
				let Some(expires_at) = entry.effective_expires_at() else {
					continue;
				};
				let expires_at = match (expires_at, read_expires_at) {
					(None, expires_at) | (expires_at, None) => expires_at,
					(Some(left), Some(right)) => Some(left.min(right)),
				};
				permissions.push(crate::permission::PermissionEntry {
					creator: None,
					grant: false,
					direct: Some(expires_at),
					materialized: None,
					permission: entry.permission,
					subject: subject.clone(),
				});
			}
		}
		Ok(permissions)
	}

	pub(crate) fn try_get_visibility_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<bool> {
		let resource_bytes = resource.to_bytes();
		let prefix = &(
			Kind::Visibility.to_i32().unwrap(),
			resource_bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, prefix);
		let mut iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the visibility entries"))?;
		let Some(entry) = iter.next() else {
			return Ok(false);
		};
		entry.map_err(|error| tg::error!(!error, "failed to read the visibility entry"))?;
		Ok(true)
	}
}
