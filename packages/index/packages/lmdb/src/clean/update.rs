use {
	crate::{Db, Index, Kind, update::Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::ops::Bound,
	tangram_client::prelude::*,
};

impl Index {
	pub(super) fn clean_update_propagated_versions(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		batch_size: usize,
	) -> tg::Result<usize> {
		if batch_size == 0 {
			return Ok(0);
		}

		// An older update can still propagate to an item.
		let oldest = Self::try_get_oldest_update_transaction_id_with_transaction(
			db,
			subspace,
			transaction,
			tangram_index::update::Kind::Permission,
		)?;

		// Read the clean keys for versions older than the oldest update.
		let key_kind = Kind::PermissionUpdateClean.to_i32().unwrap();
		let begin = Self::pack(subspace, &(key_kind,));
		let end = if let Some(oldest) = oldest {
			Self::pack(subspace, &(key_kind, oldest))
		} else {
			fdbt::Subspace::from_bytes(begin.clone()).range().1
		};
		let range = (
			Bound::Included(begin.as_slice()),
			Bound::Excluded(end.as_slice()),
		);
		let entries = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to get the update clean keys"))?;
		let mut candidates = Vec::new();
		for entry in entries.take(batch_size) {
			let (clean_key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read an update clean key"))?;
			let crate::Key::Update(Key::Clean { id, kind, version }) =
				Self::unpack(subspace, clean_key)?
			else {
				return Err(tg::error!("expected an update clean key"));
			};
			let key = crate::Key::Update(Key::PropagatedVersion { id, kind });
			let key = Self::pack(subspace, &key);
			candidates.push((clean_key.to_vec(), key, version));
		}

		for (clean_key, key, version) in &candidates {
			let value = db.get(transaction, key).map_err(|error| {
				tg::error!(!error, "failed to get the propagated update version")
			})?;
			// A stale cleanup entry must not delete a version recorded by a subsequent propagation.
			if value.is_some_and(|value| value == version.to_be_bytes()) {
				db.delete(transaction, key).map_err(|error| {
					tg::error!(!error, "failed to delete the propagated update version")
				})?;
			}
			db.delete(transaction, clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete the update clean key"))?;
		}
		let count = candidates.len();

		Ok(count)
	}
}
