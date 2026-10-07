use {
	crate::{
		Index, Key, Kind,
		permission::{PermissionIndexEntry, PermissionSource, PermissionValue},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn delete_subject_permissions_with_transaction(
		txn: &crate::Transaction,
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
			let Key::Permission(crate::permission::Key::SubjectPermission {
				creator,
				permission,
				resource,
				subject,
			}) = key
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
			for source in [
				PermissionSource::Direct,
				PermissionSource::Grant,
				PermissionSource::Materialized,
			] {
				let Some(expires_at) = value.source_expires_at(source) else {
					continue;
				};
				let entry = PermissionIndexEntry {
					creator: creator.as_ref(),
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
			Self::enqueue_permission_update(
				txn,
				subspace,
				&resource,
				&subject,
				permission,
				partition_totals.permission_update,
			);
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_permissions_with_transaction(
		txn: &crate::Transaction,
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
							creator: arg.creator.as_ref(),
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
		txn: &crate::Transaction,
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
				creator: entry.creator.cloned(),
				permission: entry.permission,
			},
		))
		.chain(std::iter::once(Key::Permission(
			crate::permission::Key::SubjectPermission {
				subject: entry.subject.clone(),
				resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			},
		)))
		.collect::<Vec<_>>();
		for key in keys {
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let Some(value) = crate::retry!(result) else {
				continue;
			};
			let mut value = PermissionValue::deserialize(&value)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if !value.delete(source, entry.expires_at) {
				continue;
			}
			if value.is_empty() {
				txn.clear(&key);
			} else {
				let bytes = value.serialize()?;
				txn.set(&key, &bytes);
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

		for id in crate::propagate!(
			Self::ancestor_ids_with_transaction(txn, subspace, entry.resource).await
		) {
			let key = Key::Permission(crate::permission::Key::Visibility {
				resource: id,
				subject: entry.subject.clone(),
				permission_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let Some(value) = crate::retry!(result) else {
				continue;
			};
			let mut value = PermissionValue::deserialize(&value)?;
			if !value.delete(source, entry.expires_at) {
				continue;
			}
			if value.is_empty() {
				txn.clear(&key);
			} else {
				let bytes = value.serialize()?;
				txn.set(&key, &bytes);
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
