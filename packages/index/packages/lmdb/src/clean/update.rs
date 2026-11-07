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
		let mut candidates = Vec::new();
		for (kind, key_kind) in [
			(
				tangram_index::update::Kind::Permission,
				Kind::PermissionUpdateClean,
			),
			(
				tangram_index::update::Kind::StorageAndMetadata,
				Kind::StorageAndMetadataUpdateClean,
			),
		] {
			let remaining = batch_size.saturating_sub(candidates.len());
			if remaining == 0 {
				break;
			}
			let oldest = Self::try_get_oldest_update_transaction_id_with_transaction(
				db,
				subspace,
				transaction,
				kind,
			)?;
			let key_kind = key_kind.to_i32().unwrap();
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
			for entry in entries.take(remaining) {
				let (clean_key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read an update clean key"))?;
				let crate::Key::Update(Key::Clean { id, kind, version }) =
					Self::unpack(subspace, clean_key)?
				else {
					return Err(tg::error!("expected an update clean key"));
				};
				let key = crate::Key::Update(Key::PropagatedVersion {
					id: id.clone(),
					kind: kind.clone(),
				});
				let key = Self::pack(subspace, &key);
				candidates.push((clean_key.to_vec(), key, version, id, kind));
			}
		}

		for (clean_key, key, version, id, kind) in &candidates {
			let value = db.get(transaction, key).map_err(|error| {
				tg::error!(!error, "failed to get the propagated update version")
			})?;
			// A stale cleanup entry must not delete a version recorded by a subsequent propagation.
			if value.is_some_and(|value| value == version.to_be_bytes()) {
				db.delete(transaction, key).map_err(|error| {
					tg::error!(!error, "failed to delete the propagated update version")
				})?;

				// Delete the propagations from a complete item, since an older update can no longer propagate through it.
				if *kind == crate::update::Kind::StorageAndMetadata {
					Self::delete_complete_item_propagations(db, subspace, transaction, id)?;
				}
			}
			db.delete(transaction, clean_key)
				.map_err(|error| tg::error!(!error, "failed to delete the update clean key"))?;
		}
		let count = candidates.len();

		Ok(count)
	}

	fn delete_complete_item_propagations(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<()> {
		// Determine whether the item is complete.
		let complete = match id {
			tg::Either::Left(id) => {
				Self::try_get_object_with_transaction(db, subspace, transaction, id)?
					.is_some_and(|object| object.complete())
			},
			tg::Either::Right(id) => {
				Self::try_get_process_with_transaction(db, subspace, transaction, id)?
					.is_some_and(|process| process.complete())
			},
		};
		if !complete {
			return Ok(());
		}

		// Delete the propagations from the item.
		let id_bytes = match id {
			tg::Either::Left(id) => id.to_bytes(),
			tg::Either::Right(id) => id.to_bytes(),
		};
		let prefix = Self::pack(
			subspace,
			&(Kind::Propagation.to_i32().unwrap(), id_bytes.as_ref()),
		);
		let (_, end) = fdbt::Subspace::from_bytes(prefix.clone()).range();
		let range = (
			Bound::Included(prefix.as_slice()),
			Bound::Excluded(end.as_slice()),
		);
		db.delete_range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to delete the propagations"))?;

		Ok(())
	}
}
