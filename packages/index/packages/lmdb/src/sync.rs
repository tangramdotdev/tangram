use {
	crate::{Db, Index, Key as IndexKey, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub enum Key {
	ExpiresAt { expires_at: i64, id: tg::sync::Id },
	Sync { id: tg::sync::Id },
}

impl Index {
	pub async fn try_get_syncs(
		&self,
		ids: &[tg::sync::Id],
	) -> tg::Result<Vec<Option<tangram_index::sync::Sync>>> {
		if ids.is_empty() {
			return Ok(Vec::new());
		}
		let request = tangram_index::read::Request::TryGetSyncs {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetSyncs(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_syncs_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::sync::Id],
	) -> tg::Result<Vec<Option<tangram_index::sync::Sync>>> {
		ids.iter()
			.map(|id| Self::try_get_sync_with_transaction(db, subspace, transaction, id))
			.collect()
	}

	fn try_get_sync_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::sync::Id,
	) -> tg::Result<Option<tangram_index::sync::Sync>> {
		let key = IndexKey::Sync(Key::Sync { id: id.clone() });
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the sync"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		let sync = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the sync"))?;

		Ok(Some(sync))
	}

	pub(crate) fn put_sync_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::sync::put::Arg,
	) -> tg::Result<()> {
		// Keep the later expiration, and remove the expiration key of the entry that it replaces.
		let id = &arg.id;
		if let Some(sync) = Self::try_get_sync_with_transaction(db, subspace, transaction, id)? {
			if sync.expires_at >= arg.expires_at {
				return Ok(());
			}
			let key = IndexKey::Sync(Key::ExpiresAt {
				expires_at: sync.expires_at,
				id: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the sync expiration"))?;
		}

		// Write the entry and its expiration key.
		let sync = tangram_index::sync::Sync {
			expires_at: arg.expires_at,
		};
		let value = tangram_serialize::to_vec(&sync)
			.map_err(|error| tg::error!(!error, %id, "failed to serialize the sync"))?;
		let key = Self::pack(subspace, &IndexKey::Sync(Key::Sync { id: id.clone() }));
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, %id, "failed to write the sync"))?;
		let key = IndexKey::Sync(Key::ExpiresAt {
			expires_at: arg.expires_at,
			id: id.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, %id, "failed to write the sync expiration"))?;

		Ok(())
	}

	pub(crate) fn delete_expired_syncs(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		now: i64,
		limit: usize,
	) -> tg::Result<usize> {
		if limit == 0 {
			return Ok(0);
		}

		// Collect the expired entries in expiration order.
		let prefix = Self::pack(subspace, &(Kind::SyncExpiresAt.to_i32().unwrap(),));
		let mut expired = Vec::new();
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the sync expirations"))?;
		for entry in entries.take(limit) {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read a sync expiration"))?;
			let IndexKey::Sync(Key::ExpiresAt { expires_at, id }) = Self::unpack(subspace, key)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			if expires_at > now {
				break;
			}
			expired.push((expires_at, id));
		}

		// Delete each expired entry and its expiration key.
		for (expires_at, id) in &expired {
			let key = Self::pack(subspace, &IndexKey::Sync(Key::Sync { id: id.clone() }));
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the sync"))?;
			let key = IndexKey::Sync(Key::ExpiresAt {
				expires_at: *expires_at,
				id: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the sync expiration"))?;
		}

		Ok(expired.len())
	}
}

impl tangram_index::sync::Index for Index {
	async fn try_get_syncs(
		&self,
		ids: &[tg::sync::Id],
	) -> tg::Result<Vec<Option<tangram_index::sync::Sync>>> {
		self.try_get_syncs(ids).await
	}
}
