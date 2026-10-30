use {
	crate::{Index, Key as IndexKey, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, pin_mut},
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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

	pub(crate) async fn try_get_syncs_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::sync::Id],
	) -> tg::Result<ControlFlow<Vec<Option<tangram_index::sync::Sync>>, fdb::FdbError>> {
		let results = futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_sync_with_transaction(txn, subspace, id)),
		)
		.await?;
		let mut output = Vec::with_capacity(results.len());
		for result in results {
			let sync = match result {
				ControlFlow::Break(sync) => sync,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			output.push(sync);
		}

		Ok(ControlFlow::Break(output))
	}

	async fn try_get_sync_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::sync::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::sync::Sync>, fdb::FdbError>> {
		let key = IndexKey::Sync(Key::Sync { id: id.clone() });
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let sync = tangram_serialize::from_slice(&bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the sync"))?;

		Ok(ControlFlow::Break(Some(sync)))
	}

	pub(crate) async fn put_sync_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::sync::put::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Keep the later expiration, and remove the expiration key of the entry that it replaces.
		let id = &arg.id;
		let sync = crate::propagate!(Self::try_get_sync_with_transaction(txn, subspace, id).await);
		if let Some(sync) = sync {
			if sync.expires_at >= arg.expires_at {
				return Ok(ControlFlow::Break(()));
			}
			let key = IndexKey::Sync(Key::ExpiresAt {
				expires_at: sync.expires_at,
				id: id.clone(),
			});
			txn.clear(&Self::pack(subspace, &key));
		}

		// Write the entry and its expiration key.
		let sync = tangram_index::sync::Sync {
			expires_at: arg.expires_at,
		};
		let value = tangram_serialize::to_vec(&sync)
			.map_err(|error| tg::error!(!error, %id, "failed to serialize the sync"))?;
		let key = IndexKey::Sync(Key::Sync { id: id.clone() });
		txn.set(&Self::pack(subspace, &key), &value);
		let key = IndexKey::Sync(Key::ExpiresAt {
			expires_at: arg.expires_at,
			id: id.clone(),
		});
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_expired_syncs(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		now: i64,
		limit: usize,
	) -> tg::Result<ControlFlow<usize, fdb::FdbError>> {
		if limit == 0 {
			return Ok(ControlFlow::Break(0));
		}

		// Read the expiration keys in expiration order.
		let prefix = Self::pack(subspace, &(Kind::SyncExpiresAt.to_i32().unwrap(),));
		let range = fdb::RangeOption {
			limit: Some(limit),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
		};
		let entries = txn.get_ranges_keyvalues(range, false);
		pin_mut!(entries);
		let mut expired = Vec::new();
		while let Some(result) = entries.next().await {
			let entry = crate::retry!(result);
			let IndexKey::Sync(Key::ExpiresAt { expires_at, id }) =
				Self::unpack(subspace, entry.key())?
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
			let key = IndexKey::Sync(Key::Sync { id: id.clone() });
			txn.clear(&Self::pack(subspace, &key));
			let key = IndexKey::Sync(Key::ExpiresAt {
				expires_at: *expires_at,
				id: id.clone(),
			});
			txn.clear(&Self::pack(subspace, &key));
		}

		Ok(ControlFlow::Break(expired.len()))
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
