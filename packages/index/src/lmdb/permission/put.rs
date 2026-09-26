use {
	crate::lmdb::{
		Db, Index, Key, Request, Response,
		permission::{PermissionIndexEntry, PermissionSource, PermissionValue},
	},
	foundationdb_tuple as fdbt, heed as lmdb,
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

	pub(crate) fn put_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::permission::put::Arg],
	) -> tg::Result<()> {
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
				let entry = PermissionIndexEntry {
					creator: arg.creator.as_ref(),
					expires_at,
					permission,
					subject: &arg.subject,
					resource: &arg.resource,
				};
				let changed = Self::put_permission_index_entry(
					db,
					subspace,
					transaction,
					&entry,
					source,
					arg.time_to_touch,
				)?;
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

	pub(crate) fn put_permission_index_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &PermissionIndexEntry<'_>,
		source: PermissionSource,
		time_to_touch: Option<std::time::Duration>,
	) -> tg::Result<bool> {
		let mut changed = false;
		let keys = std::iter::once(Key::Permission(
			crate::lmdb::permission::Key::ResourcePermission {
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			},
		))
		.chain(std::iter::once(Key::Permission(
			crate::lmdb::permission::Key::SubjectPermission {
				subject: entry.subject.clone(),
				resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			},
		)))
		.collect::<Vec<_>>();
		for key in keys {
			let key = Self::pack(subspace, &key);
			let mut value = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the permission entry"))?
				.map_or_else(
					|| Ok(PermissionValue::default()),
					PermissionValue::deserialize,
				)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if value.put(source, entry.expires_at, time_to_touch) {
				let bytes = value.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put the permission entry"))?;
				Self::update_permission_expiration(
					db,
					subspace,
					transaction,
					entry,
					source,
					old_expires_at,
					entry.expires_at,
				)?;
				changed = true;
			}
		}
		if !changed {
			return Ok(false);
		}

		let ids = Self::ancestor_ids_with_transaction(db, subspace, transaction, entry.resource)?;
		for id in ids {
			let key = Key::Permission(crate::lmdb::permission::Key::Visibility {
				resource: id,
				subject: entry.subject.clone(),
				permission_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let mut value = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the visibility entry"))?
				.map_or_else(
					|| Ok(PermissionValue::default()),
					PermissionValue::deserialize,
				)?;
			if value.put(source, entry.expires_at, time_to_touch) {
				let bytes = value.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put the visibility entry"))?;
			}
		}
		Ok(changed)
	}

	pub(crate) fn update_permission_expiration(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &PermissionIndexEntry<'_>,
		source: PermissionSource,
		old_expires_at: Option<i64>,
		new_expires_at: Option<i64>,
	) -> tg::Result<()> {
		if let Some(expires_at) = old_expires_at {
			let key = Key::Permission(crate::lmdb::permission::Key::PermissionExpiresAt {
				expires_at,
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key).map_err(|error| {
				tg::error!(!error, "failed to delete the permission expiration")
			})?;
		}
		if let Some(expires_at) = new_expires_at {
			let key = Key::Permission(crate::lmdb::permission::Key::PermissionExpiresAt {
				expires_at,
				resource: entry.resource.clone(),
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the permission expiration"))?;
		}
		Ok(())
	}

	pub(crate) fn enqueue_permission_update(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
		permission: tg::authorization::Permission,
	) -> tg::Result<()> {
		match permission {
			tg::authorization::Permission::Object(_) => {
				if let Ok(id) = tg::object::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						db,
						subspace,
						transaction,
						tg::Either::Left(id),
						crate::lmdb::update::Kind::Permission(subject.clone()),
						crate::lmdb::update::Source::Put,
						None,
					)?;
				}
			},
			tg::authorization::Permission::Process(_) => {
				if let Ok(id) = tg::process::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						db,
						subspace,
						transaction,
						tg::Either::Right(id),
						crate::lmdb::update::Kind::Permission(subject.clone()),
						crate::lmdb::update::Source::Put,
						None,
					)?;
				}
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Sync(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => {},
		}
		Ok(())
	}
}
