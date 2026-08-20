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
	pub async fn delete_grants(&self, args: &[crate::grant::delete::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteGrants(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn delete_grants_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::grant::delete::Arg],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			for permission in arg.permissions.iter() {
				let (expires_at, source) = match arg.implicit {
					None => (None, GrantSource::Explicit),
					Some(expires_at) => (expires_at, GrantSource::Implicit),
				};
				let changed = crate::fdb::propagate!(
					Self::delete_grant_index_entry(
						txn,
						subspace,
						&GrantIndexEntry {
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

	pub(crate) async fn delete_grant_index_entry(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &GrantIndexEntry<'_>,
		source: GrantSource,
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
			let Some(value) = crate::fdb::retry!(result) else {
				continue;
			};
			let mut value = GrantValue::deserialize(&value)?;
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
			Self::update_grant_expiration(
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
			let key = Key::Grant(crate::fdb::grant::Key::Visibility {
				resource: id,
				subject: entry.subject.clone(),
				grant_resource: entry.resource.clone(),
				creator: entry.creator.cloned(),
				permission: entry.permission,
			});
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let Some(value) = crate::fdb::retry!(result) else {
				continue;
			};
			let mut value = GrantValue::deserialize(&value)?;
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
		Self::update_grant_expiration(
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
