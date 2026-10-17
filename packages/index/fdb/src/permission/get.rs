use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn get_resource_permission_subjects_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
	) -> tg::Result<ControlFlow<BTreeSet<tg::authorization::Subject>, fdb::FdbError>> {
		let bytes = resource.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ResourcePermission.to_i32().unwrap(), bytes.as_ref()),
		);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption::from(&range_subspace);
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let mut subjects = BTreeSet::new();
		for entry in entries {
			let Key::Permission(crate::permission::Key::ResourcePermission { subject, .. }) =
				Self::unpack(subspace, entry.key())?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			subjects.insert(subject);
		}
		Ok(ControlFlow::Break(subjects))
	}

	pub(crate) async fn get_resource_permission_entries_for_subject_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<Vec<crate::permission::PermissionEntry>, fdb::FdbError>> {
		let bytes = resource.to_bytes();
		let key = (
			Kind::ResourcePermission.to_i32().unwrap(),
			bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);

		let entries = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Permission(crate::permission::Key::ResourcePermission {
					creator,
					permission,
					subject,
					..
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let value = crate::permission::PermissionValue::deserialize(entry.value())?;
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

		Ok(ControlFlow::Break(entries))
	}

	pub(crate) async fn get_resource_sync_permission_entries_for_subject_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<Vec<crate::permission::PermissionEntry>, fdb::FdbError>> {
		let read = tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		);
		let subjects = crate::propagate!(
			Self::get_resource_permission_subjects_with_transaction(txn, subspace, resource).await
		);
		let mut permissions = Vec::new();
		for sync_subject in subjects {
			let tg::authorization::Subject::Sync(sync) = &sync_subject else {
				continue;
			};
			let sync_resource = tg::Id::from(sync.clone());
			let read_entries = crate::propagate!(
				Self::get_resource_permission_entries_for_subject_with_transaction(
					txn,
					subspace,
					&sync_resource,
					subject,
				)
				.await
			);
			let Some(read_expires_at) = read_entries
				.iter()
				.filter(|entry| entry.permission == read)
				.filter_map(|entry| entry.effective_expires_at())
				.reduce(crate::permission::max_expires_at)
			else {
				continue;
			};
			let entries = crate::propagate!(
				Self::get_resource_permission_entries_for_subject_with_transaction(
					txn,
					subspace,
					resource,
					&sync_subject,
				)
				.await
			);
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

		Ok(ControlFlow::Break(permissions))
	}

	pub(crate) async fn try_get_visibility_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let bytes = resource.to_bytes();
		let key = (
			Kind::Visibility.to_i32().unwrap(),
			bytes.as_ref(),
			subject.to_string(),
		);
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			limit: Some(1),
			..fdb::RangeOption::from(&range_subspace)
		};
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);

		Ok(ControlFlow::Break(!entries.is_empty()))
	}
}
