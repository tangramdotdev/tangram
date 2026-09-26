use {
	crate::fdb::{
		Index, Key, Request, Response,
		permission::{PermissionIndexEntry, PermissionSource, PermissionValue},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
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

	pub(crate) async fn delete_permissions_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::permission::delete::Arg],
		partition_totals: crate::fdb::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		for arg in args {
			for permission in arg.permissions.iter() {
				let (expires_at, source) = match arg.source {
					crate::permission::Source::Direct { expires_at } => {
						(expires_at, PermissionSource::Direct)
					},
					crate::permission::Source::Grant => (None, PermissionSource::Grant),
				};
				let changed = crate::fdb::propagate!(
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
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &PermissionIndexEntry<'_>,
		source: PermissionSource,
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let mut changed = false;
		let keys = std::iter::once(Key::Permission(
			crate::fdb::permission::Key::ResourcePermission {
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			},
		))
		.chain(std::iter::once(Key::Permission(
			crate::fdb::permission::Key::SubjectPermission {
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
			let Some(value) = crate::fdb::retry!(result) else {
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

		for id in crate::fdb::propagate!(
			Self::ancestor_ids_with_transaction(txn, subspace, entry.resource).await
		) {
			let key = Key::Permission(crate::fdb::permission::Key::Visibility {
				resource: id,
				subject: entry.subject.clone(),
				permission_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let Some(value) = crate::fdb::retry!(result) else {
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
