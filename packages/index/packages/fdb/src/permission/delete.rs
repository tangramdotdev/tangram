use {
	crate::{
		Index, Key, Kind,
		permission::{PermissionIndexEntry, PermissionSource, PermissionValue},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{TryStreamExt as _, future},
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn delete_subject_permissions_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		subject: &tg::authorization::Subject,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SubjectPermission.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let range_subspace = fdbt::Subspace::from_bytes(prefix);
		let range = fdb::RangeOption::from(&range_subspace);
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let entries = entries
			.iter()
			.map(|entry| {
				Ok((
					Self::unpack(subspace, entry.key())?,
					PermissionValue::deserialize(entry.value())?,
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
					crate::propagate!(
						Self::schedule_object_accounts_for_cleaning(
							txn,
							subspace,
							&object,
							partition_totals.usage_update
						)
						.await
					);
				} else if let Ok(process) = tg::process::Id::try_from(resource.clone()) {
					crate::propagate!(
						Self::schedule_process_accounts_for_cleaning(
							txn,
							subspace,
							&process,
							partition_totals.usage_update
						)
						.await
					);
				}
			}
			for (permission, source, expires_at) in value.source_expirations() {
				let entry = PermissionIndexEntry {
					expires_at,
					permission,
					resource: &resource,
					subject: &subject,
				};
				crate::propagate!(
					Self::delete_permission_index_entry(
						txn,
						subspace,
						&entry,
						source,
						partition_totals.cleaning
					)
					.await
				);
			}
			for permission in value.permissions.keys() {
				Self::enqueue_permission_update(
					txn,
					subspace,
					&resource,
					&subject,
					*permission,
					partition_totals.permission_update,
				);
			}
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_permissions_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::permission::delete::Arg],
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		for arg in args {
			for permission in arg.permissions.iter() {
				let (expires_at, source) = match arg.source {
					tangram_index::permission::Source::Direct { expires_at } => {
						(expires_at, PermissionSource::Direct)
					},
					tangram_index::permission::Source::Grant => (None, PermissionSource::Grant),
				};
				let changed = crate::propagate!(
					Self::delete_permission_index_entry(
						txn,
						subspace,
						&PermissionIndexEntry {
							expires_at,
							permission,
							subject: &arg.subject,
							resource: &arg.resource,
						},
						source,
						partition_total,
					)
					.await
				);
				if changed {
					Self::enqueue_permission_update(
						txn,
						subspace,
						&arg.resource,
						&arg.subject,
						permission,
						partition_totals.permission_update,
					);
				}
			}
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_permission_index_entry(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &PermissionIndexEntry<'_>,
		source: PermissionSource,
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
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
		.map(|key| Self::pack(subspace, &key))
		.collect::<Vec<_>>();

		// Read the permission entries and the ancestors.
		let (results, ids) = futures::join!(
			future::join_all(keys.iter().map(|key| txn.get(key, false))),
			Self::ancestor_ids_with_transaction(txn, subspace, entry.resource),
		);

		// Delete the permission entries.
		let mut empty = true;
		for (key, result) in keys.iter().zip(results) {
			let Some(value) = crate::retry!(result) else {
				continue;
			};
			let mut value = PermissionValue::deserialize(&value)?;
			let old_expires_at = value.source_expires_at(entry.permission, source).flatten();
			let deleted = value.delete(entry.permission, source, entry.expires_at);
			empty = empty && value.is_empty();
			if !deleted {
				continue;
			}
			if value.is_empty() {
				txn.clear(key);
			} else {
				let bytes = value.serialize()?;
				txn.set(key, &bytes);
			}
			Self::update_permission_expiration(
				txn,
				subspace,
				entry,
				source,
				old_expires_at,
				None,
				partition_total,
			);
			changed = true;
		}

		// Delete the visibility entries of the ancestors if the subject has no remaining permissions on the resource.
		let ids = crate::propagate!(ids);
		if empty {
			for id in ids {
				let key = Key::Permission(crate::permission::Key::Visibility {
					resource: id,
					subject: entry.subject.clone(),
					permission_resource: entry.resource.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}
		}

		Self::update_permission_expiration(
			txn,
			subspace,
			entry,
			source,
			entry.expires_at,
			None,
			partition_total,
		);
		Ok(ControlFlow::Break(changed))
	}
}
