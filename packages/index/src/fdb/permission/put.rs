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
	pub async fn put_permissions(&self, args: &[crate::permission::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutPermissions(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn put_permissions_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::permission::put::Arg],
		partition_totals: crate::fdb::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		for arg in args {
			let (expires_at, source) = match arg.source {
				crate::permission::Source::Direct { expires_at } => {
					(expires_at, PermissionSource::Direct)
				},
				crate::permission::Source::Grant => (None, PermissionSource::Grant),
			};
			let non_expiring_direct = matches!(
				arg.source,
				crate::permission::Source::Direct { expires_at: None }
			);
			if non_expiring_direct {
				let tg::authorization::Subject::Process(process) = &arg.subject else {
					return Err(tg::error!(
						"a non-expiring direct permission must have a process subject"
					));
				};
				if arg.creator.as_ref() != Some(&tg::Principal::Process(process.clone())) {
					return Err(tg::error!(
						"a non-expiring direct permission must be created by its process"
					));
				}
				if tg::object::Id::try_from(arg.resource.clone()).is_err() {
					return Err(tg::error!(
						"a non-expiring direct permission must target an object"
					));
				}
			}
			for permission in arg.permissions.iter() {
				if non_expiring_direct
					&& !matches!(permission, tg::authorization::Permission::Object(_))
				{
					return Err(tg::error!(
						"a non-expiring direct permission must contain object permissions"
					));
				}
				let changed = crate::fdb::propagate!(
					Self::put_permission_index_entry(
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
						arg.time_to_touch,
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

	pub(crate) async fn put_permission_index_entry(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &PermissionIndexEntry<'_>,
		source: PermissionSource,
		time_to_touch: Option<std::time::Duration>,
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
			let mut value = crate::fdb::retry!(result).as_deref().map_or_else(
				|| Ok(PermissionValue::default()),
				PermissionValue::deserialize,
			)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if value.put(source, entry.expires_at, time_to_touch) {
				let bytes = value.serialize()?;
				txn.set(&key, &bytes);
				Self::update_permission_expiration(
					txn,
					subspace,
					entry,
					source,
					old_expires_at,
					entry.expires_at,
					partition_total,
				);
				changed = true;
			}
		}
		if !changed {
			return Ok(ControlFlow::Break(false));
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
			let mut value = crate::fdb::retry!(result).as_deref().map_or_else(
				|| Ok(PermissionValue::default()),
				PermissionValue::deserialize,
			)?;
			if value.put(source, entry.expires_at, time_to_touch) {
				let bytes = value.serialize()?;
				txn.set(&key, &bytes);
			}
		}
		Ok(ControlFlow::Break(changed))
	}

	pub(crate) fn update_permission_expiration(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &PermissionIndexEntry<'_>,
		source: PermissionSource,
		old_expires_at: Option<i64>,
		new_expires_at: Option<i64>,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(&entry.resource.to_bytes(), partition_total);
		if let Some(expires_at) = old_expires_at {
			let key = Key::Permission(crate::fdb::permission::Key::PermissionExpiresAt {
				partition,
				expires_at,
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		if let Some(expires_at) = new_expires_at {
			let key = Key::Permission(crate::fdb::permission::Key::PermissionExpiresAt {
				partition,
				expires_at,
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
	}

	pub(crate) fn enqueue_permission_update(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
		permission: tg::authorization::Permission,
		partition_total: u64,
	) {
		match permission {
			tg::authorization::Permission::Object(_) => {
				if let Ok(id) = tg::object::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						txn,
						subspace,
						&tg::Either::Left(id),
						&crate::fdb::update::Kind::Permission(subject.clone()),
						crate::fdb::update::Source::Put,
						partition_total,
					);
				}
			},
			tg::authorization::Permission::Process(_) => {
				if let Ok(id) = tg::process::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						txn,
						subspace,
						&tg::Either::Right(id),
						&crate::fdb::update::Kind::Permission(subject.clone()),
						crate::fdb::update::Source::Put,
						partition_total,
					);
				}
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Sync(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => {},
		}
	}
}
