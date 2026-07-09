use {
	crate::fdb::{
		Index, Key, Request, Response,
		grant::{GrantIndexEntry, GrantSource, GrantValue},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
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
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(())
	}

	pub(crate) async fn task_put_grants(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::grant::put::Arg],
		partition_total: u64,
	) -> tg::Result<()> {
		for arg in args {
			for permission in arg.permissions.iter() {
				let source = if arg.expires_at.is_some() {
					GrantSource::Temporary
				} else {
					GrantSource::Explicit
				};
				let changed = Self::put_grant_index_entry(
					txn,
					subspace,
					&GrantIndexEntry {
						creator: arg.creator.as_ref(),
						expires_at: arg.expires_at,
						permission,
						principal: &arg.principal,
						resource: &arg.resource,
					},
					source,
					partition_total,
				)
				.await?;
				if changed {
					Self::enqueue_grant_update(
						txn,
						subspace,
						&arg.resource,
						&arg.principal,
						permission,
						partition_total,
					);
				}
			}
		}
		Ok(())
	}

	pub(crate) async fn put_grant_index_entry(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
		partition_total: u64,
	) -> tg::Result<bool> {
		let mut changed = false;
		let keys = std::iter::once(Key::Grant(crate::fdb::grant::Key::ResourceGrant {
			resource: entry.resource.clone(),
			principal: entry.principal.clone(),
			creator: entry.creator.cloned(),
			permission: entry.permission,
		}))
		.chain(std::iter::once(Key::Grant(
			crate::fdb::grant::Key::PrincipalGrant {
				principal: entry.principal.clone(),
				resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			},
		)))
		.collect::<Vec<_>>();
		for key in keys {
			let key = Self::pack(subspace, &key);
			let mut value = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the grant entry"))?
				.as_deref()
				.map_or_else(|| Ok(GrantValue::default()), GrantValue::deserialize)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if value.put(source, entry.expires_at) {
				let bytes = value.serialize()?;
				txn.set(&key, &bytes);
				Self::update_grant_expiration(
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

		for id in Self::ancestor_ids_with_transaction(txn, subspace, entry.resource).await? {
			let key = Key::Grant(crate::fdb::grant::Key::Visibility {
				resource: id,
				principal: entry.principal.clone(),
				grant_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let mut value = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the visibility entry"))?
				.as_deref()
				.map_or_else(|| Ok(GrantValue::default()), GrantValue::deserialize)?;
			if value.put(source, entry.expires_at) {
				let bytes = value.serialize()?;
				txn.set(&key, &bytes);
			}
		}
		Ok(changed)
	}

	pub(crate) fn update_grant_expiration(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
		old_expires_at: Option<i64>,
		new_expires_at: Option<i64>,
		partition_total: u64,
	) {
		let partition = Self::partition_for_id(&entry.resource.to_bytes(), partition_total);
		if let Some(expires_at) = old_expires_at {
			let key = Key::Grant(crate::fdb::grant::Key::GrantExpiresAt {
				partition,
				expires_at,
				resource: entry.resource.clone(),
				principal: entry.principal.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		if let Some(expires_at) = new_expires_at {
			let key = Key::Grant(crate::fdb::grant::Key::GrantExpiresAt {
				partition,
				expires_at,
				resource: entry.resource.clone(),
				principal: entry.principal.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
	}

	pub(crate) fn enqueue_grant_update(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
		principal: &tg::grant::Principal,
		permission: tg::grant::Permission,
		partition_total: u64,
	) {
		match permission {
			tg::grant::Permission::Object(_) => {
				if let Ok(id) = tg::object::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						txn,
						subspace,
						&tg::Either::Left(id),
						crate::fdb::update::Kind::Grants(principal.clone()),
						crate::fdb::update::Source::Put,
						partition_total,
					);
				}
			},
			tg::grant::Permission::Process(_) => {
				if let Ok(id) = tg::process::Id::try_from(resource.clone()) {
					Self::enqueue_update_with_kind(
						txn,
						subspace,
						&tg::Either::Right(id),
						crate::fdb::update::Kind::Grants(principal.clone()),
						crate::fdb::update::Source::Put,
						partition_total,
					);
				}
			},
			tg::grant::Permission::Group(_)
			| tg::grant::Permission::Organization(_)
			| tg::grant::Permission::Sandbox(_)
			| tg::grant::Permission::Tag(_)
			| tg::grant::Permission::User(_) => {},
		}
	}
}
