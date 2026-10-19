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
		crate::page::collect(|after| {
			Self::get_resource_permission_entries_for_subject_page_with_transaction(
				db,
				subspace,
				transaction,
				resource,
				subject,
				after,
				crate::page::PAGE_SIZE,
			)
		})
	}

	pub(crate) fn get_resource_permission_entries_for_subject_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<crate::page::Page<crate::permission::PermissionEntry>> {
		let resource_bytes = resource.to_bytes();
		let prefix = &(
			Kind::ResourcePermission.to_i32().unwrap(),
			resource_bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, prefix);
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let entries = entries
			.into_iter()
			.map(|(key, value)| {
				let Key::Permission(crate::permission::Key::ResourcePermission {
					creator,
					permission,
					subject,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let value = crate::permission::PermissionValue::deserialize(&value)?;
				Ok(crate::permission::PermissionEntry {
					creator,
					grant: value.grant,
					direct: value.direct,
					materialized: value.materialized,
					permission,
					subject,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((entries, cursor))
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
