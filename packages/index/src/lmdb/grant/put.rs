use {
	crate::lmdb::{
		Db, Index, Key, Request,
		grant::{GrantIndexEntry, GrantSource, GrantValue},
	},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_grants(&self, args: &[crate::grant::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutGrants(args.to_vec());
		self.sender_medium
			.as_ref()
			.unwrap()
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_put_grants(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::grant::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			for permission in arg.permissions.iter() {
				let source = if arg.expires_at.is_some() {
					GrantSource::Temporary
				} else {
					GrantSource::Explicit
				};
				Self::put_grant_index_entry(
					db,
					subspace,
					transaction,
					&GrantIndexEntry {
						creator: arg.creator.as_ref(),
						expires_at: arg.expires_at,
						permission,
						principal: &arg.principal,
						resource: &arg.resource,
					},
					source,
				)?;
				Self::enqueue_grant_update(
					db,
					subspace,
					transaction,
					&arg.resource,
					&arg.principal,
					permission,
				)?;
			}
		}
		Ok(())
	}

	pub(crate) fn put_grant_index_entry(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
	) -> tg::Result<bool> {
		let mut changed = false;
		let keys = std::iter::once(Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
			resource: entry.resource.clone(),
			principal: entry.principal.clone(),
			creator: entry.creator.cloned(),
			permission: entry.permission,
		}))
		.chain(std::iter::once(Key::Grant(
			crate::lmdb::grant::Key::PrincipalGrant {
				principal: entry.principal.clone(),
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
				.map_err(|error| tg::error!(!error, "failed to get the grant entry"))?
				.map_or_else(|| Ok(GrantValue::default()), GrantValue::deserialize)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if value.put(source, entry.expires_at) {
				let bytes = value.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put the grant entry"))?;
				Self::update_grant_expiration(
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

		let ids = Self::ancestor_ids_with_transaction(db, subspace, transaction, entry.resource)?;
		for id in ids {
			let key = Key::Grant(crate::lmdb::grant::Key::Visibility {
				resource: id,
				principal: entry.principal.clone(),
				grant_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let mut value = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the visibility entry"))?
				.map_or_else(|| Ok(GrantValue::default()), GrantValue::deserialize)?;
			if value.put(source, entry.expires_at) {
				let bytes = value.serialize()?;
				db.put(transaction, &key, &bytes)
					.map_err(|error| tg::error!(!error, "failed to put the visibility entry"))?;
			}
		}
		Ok(changed)
	}

	pub(crate) fn update_grant_expiration(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
		old_expires_at: Option<i64>,
		new_expires_at: Option<i64>,
	) -> tg::Result<()> {
		if let Some(expires_at) = old_expires_at {
			let key = Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
				expires_at,
				resource: entry.resource.clone(),
				principal: entry.principal.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the grant expiration"))?;
		}
		if let Some(expires_at) = new_expires_at {
			let key = Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
				expires_at,
				resource: entry.resource.clone(),
				principal: entry.principal.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the grant expiration"))?;
		}
		Ok(())
	}

	pub(crate) fn enqueue_grant_update(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
		principal: &tg::grant::Principal,
		permission: tg::grant::Permission,
	) -> tg::Result<()> {
		match permission {
			tg::grant::Permission::Object(_) => {
				if let Ok(id) = tg::object::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						db,
						subspace,
						transaction,
						tg::Either::Left(id),
						crate::lmdb::update::Kind::Grants(principal.clone()),
						crate::lmdb::update::Source::Put,
						None,
					)?;
				}
			},
			tg::grant::Permission::Process(_) => {
				if let Ok(id) = tg::process::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						db,
						subspace,
						transaction,
						tg::Either::Right(id),
						crate::lmdb::update::Kind::Grants(principal.clone()),
						crate::lmdb::update::Source::Put,
						None,
					)?;
				}
			},
			tg::grant::Permission::Group(_)
			| tg::grant::Permission::Organization(_)
			| tg::grant::Permission::Sandbox(_)
			| tg::grant::Permission::Tag(_)
			| tg::grant::Permission::User(_) => {},
		}
		Ok(())
	}
}
