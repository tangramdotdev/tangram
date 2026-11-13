use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn get_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		args: &[tangram_index::permission::get::Arg],
	) -> tg::Result<Vec<Vec<tg::authorization::Permission>>> {
		args.iter()
			.map(|arg| {
				let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&arg.resource,
					&arg.subject,
				)?;
				let permissions = entries.into_iter().map(|entry| entry.permission).collect();
				Ok(permissions)
			})
			.collect()
	}

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
		let key = Key::Permission(crate::permission::Key::ResourcePermission {
			resource: resource.clone(),
			subject: subject.clone(),
		});
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the resource permissions"))?;
		let entries = value
			.map(crate::permission::PermissionValue::deserialize)
			.transpose()?
			.map_or_default(|value| value.entries(subject));

		Ok(entries)
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
