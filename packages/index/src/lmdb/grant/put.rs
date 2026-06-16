use {
	crate::lmdb::{
		Db, Index, Key, Request,
		grant::{GrantIndexEntry, GrantSource, grant_source_mask, grant_sources},
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
					GrantSource::Explicit,
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
		let mask = grant_source_mask(source);
		let mut changed = false;
		let keys = std::iter::once(Key::Grant(crate::lmdb::grant::Key::ResourceGrant {
			resource: entry.resource.clone(),
			principal: entry.principal.clone(),
			creator: entry.creator.cloned(),
			permission: entry.permission,
			expires_at: entry.expires_at,
		}))
		.chain(std::iter::once(Key::Grant(
			crate::lmdb::grant::Key::PrincipalGrant {
				principal: entry.principal.clone(),
				resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				expires_at: entry.expires_at,
			},
		)))
		.collect::<Vec<_>>();
		for key in keys {
			let key = Self::pack(subspace, &key);
			let sources = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the grant entry"))?
				.map_or(0, grant_sources);
			let new_sources = sources | mask;
			if new_sources != sources {
				db.put(transaction, &key, &[new_sources])
					.map_err(|error| tg::error!(!error, "failed to put the grant entry"))?;
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
				expires_at: entry.expires_at,
			});
			let key = Self::pack(subspace, &key);
			let sources = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the visibility entry"))?
				.map_or(0, grant_sources);
			let new_sources = sources | mask;
			if new_sources != sources {
				db.put(transaction, &key, &[new_sources])
					.map_err(|error| tg::error!(!error, "failed to put the visibility entry"))?;
			}
		}
		if let Some(expires_at) = entry.expires_at {
			let key = Key::Grant(crate::lmdb::grant::Key::GrantExpiresAt {
				expires_at,
				resource: entry.resource.clone(),
				principal: entry.principal.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let sources = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the grant expiration"))?
				.map_or(0, grant_sources);
			let new_sources = sources | mask;
			if new_sources != sources {
				db.put(transaction, &key, &[new_sources])
					.map_err(|error| tg::error!(!error, "failed to put the grant expiration"))?;
			}
		}
		Ok(changed)
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
			tg::grant::Permission::Admin
			| tg::grant::Permission::Read
			| tg::grant::Permission::Write => {},
		}
		Ok(())
	}
}
