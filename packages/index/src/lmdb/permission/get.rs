use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn get_resource_permission_entries_for_subject_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<Vec<crate::lmdb::permission::PermissionEntry>> {
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
			let Key::Permission(crate::lmdb::permission::Key::ResourcePermission {
				creator,
				permission,
				subject,
				..
			}) = key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = crate::lmdb::permission::PermissionValue::deserialize(value)?;
			permissions.push(crate::lmdb::permission::PermissionEntry {
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
