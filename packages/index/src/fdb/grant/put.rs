use {
	crate::fdb::{
		Index, Key, Request, Response,
		grant::{GrantIndexEntry, GrantSource, GrantValue},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_grants(&self, args: &[crate::grant::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutGrants(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn put_grants_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::grant::put::Arg],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			for permission in arg.permissions.iter() {
				let source = if arg.expires_at.is_some() {
					GrantSource::Temporary
				} else {
					GrantSource::Explicit
				};
				let changed = crate::fdb::propagate!(
					Self::put_grant_index_entry(
						txn,
						subspace,
						&GrantIndexEntry {
							creator: arg.creator.as_ref(),
							expires_at: arg.expires_at,
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
					Self::enqueue_grant_update(
						txn,
						subspace,
						&arg.resource,
						&arg.subject,
						permission,
						partition_total,
					);
				}
			}
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_grant_index_entry(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
		time_to_touch: Option<std::time::Duration>,
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let mut changed = false;
		let keys = std::iter::once(Key::Grant(crate::fdb::grant::Key::ResourceGrant {
			resource: entry.resource.clone(),
			subject: entry.subject.clone(),
			creator: entry.creator.cloned(),
			permission: entry.permission,
		}))
		.chain(std::iter::once(Key::Grant(
			crate::fdb::grant::Key::SubjectGrant {
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
			let mut value = crate::fdb::retry!(result)
				.as_deref()
				.map_or_else(|| Ok(GrantValue::default()), GrantValue::deserialize)?;
			let old_expires_at = value.source_expires_at(source).flatten();
			if value.put(source, entry.expires_at, time_to_touch) {
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
		if !changed {
			return Ok(ControlFlow::Break(false));
		}

		for id in crate::fdb::propagate!(
			Self::ancestor_ids_with_transaction(txn, subspace, entry.resource).await
		) {
			let key = Key::Grant(crate::fdb::grant::Key::Visibility {
				resource: id,
				subject: entry.subject.clone(),
				grant_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let mut value = crate::fdb::retry!(result)
				.as_deref()
				.map_or_else(|| Ok(GrantValue::default()), GrantValue::deserialize)?;
			if value.put(source, entry.expires_at, time_to_touch) {
				let bytes = value.serialize()?;
				txn.set(&key, &bytes);
			}
		}
		Ok(ControlFlow::Break(changed))
	}

	pub(crate) fn update_grant_expiration(
		txn: &crate::fdb::Transaction,
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
				subject: entry.subject.clone(),
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
				subject: entry.subject.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
				source,
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
	}

	pub(crate) fn enqueue_grant_update(
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
						&crate::fdb::update::Kind::Grant(subject.clone()),
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
						&crate::fdb::update::Kind::Grant(subject.clone()),
						crate::fdb::update::Source::Put,
						partition_total,
					);
				}
			},
			tg::authorization::Permission::Group(_)
			| tg::authorization::Permission::Organization(_)
			| tg::authorization::Permission::Sandbox(_)
			| tg::authorization::Permission::Tag(_)
			| tg::authorization::Permission::User(_) => {},
		}
	}
}
