use {
	super::{Index, Key, ObjectKind, ProcessKind},
	crate::{ObjectStored, ProcessStored},
	foundationdb::options::MutationType,
	foundationdb_tuple::TuplePack as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Check if the object exists by looking for the touched_at field.
		let touched_at_key = Key::ObjectField {
			id,
			field: ObjectKind::TouchedAt,
		}
		.pack_to_vec();
		let touched_at_value = txn
			.get(&touched_at_key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object touched_at"))?;
		if touched_at_value.is_none() {
			return Ok(None);
		}

		// Use atomic max to update touched_at.
		let touched_at_bytes = touched_at.to_be_bytes();
		txn.atomic_op(&touched_at_key, &touched_at_bytes, MutationType::Max);

		// Combined range scan for stored and metadata fields.
		let (stored, metadata) = Self::read_object_stored_and_metadata(&txn, id).await?;

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(Some((stored, metadata)))
	}

	pub async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let touched_at_bytes = touched_at.to_be_bytes();

		let results = futures::future::try_join_all(ids.iter().map(|id| async {
			// Check if the object exists by looking for the touched_at field.
			let touched_at_key = Key::ObjectField {
				id,
				field: ObjectKind::TouchedAt,
			}
			.pack_to_vec();
			let touched_at_value = txn
				.get(&touched_at_key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object touched_at"))?;
			if touched_at_value.is_none() {
				return Ok::<_, tg::Error>(None);
			}

			// Use atomic max to update touched_at.
			txn.atomic_op(&touched_at_key, &touched_at_bytes, MutationType::Max);

			// Combined range scan for stored and metadata fields.
			let (stored, metadata) = Self::read_object_stored_and_metadata(&txn, id).await?;

			Ok(Some((stored, metadata)))
		}))
		.await?;

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(results)
	}

	pub async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Check if the process exists by looking for the touched_at field.
		let touched_at_key = Key::ProcessField {
			id,
			field: ProcessKind::TouchedAt,
		}
		.pack_to_vec();
		let touched_at_value = txn
			.get(&touched_at_key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process touched_at"))?;
		if touched_at_value.is_none() {
			return Ok(None);
		}

		// Use atomic max to update touched_at.
		let touched_at_bytes = touched_at.to_be_bytes();
		txn.atomic_op(&touched_at_key, &touched_at_bytes, MutationType::Max);

		// Combined range scan for stored and metadata fields.
		let (stored, metadata) = Self::read_process_stored_and_metadata(&txn, id).await?;

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(Some((stored, metadata)))
	}

	pub async fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let touched_at_bytes = touched_at.to_be_bytes();

		let results = futures::future::try_join_all(ids.iter().map(|id| async {
			// Check if the process exists by looking for the touched_at field.
			let touched_at_key = Key::ProcessField {
				id,
				field: ProcessKind::TouchedAt,
			}
			.pack_to_vec();
			let touched_at_value = txn
				.get(&touched_at_key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process touched_at"))?;
			if touched_at_value.is_none() {
				return Ok::<_, tg::Error>(None);
			}

			// Use atomic max to update touched_at.
			txn.atomic_op(&touched_at_key, &touched_at_bytes, MutationType::Max);

			// Combined range scan for stored and metadata fields.
			let (stored, metadata) = Self::read_process_stored_and_metadata(&txn, id).await?;

			Ok(Some((stored, metadata)))
		}))
		.await?;

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(results)
	}

	pub async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Check if the object exists by looking for the touched_at field.
		let touched_at_key = Key::ObjectField {
			id,
			field: ObjectKind::TouchedAt,
		}
		.pack_to_vec();
		let touched_at_value = txn
			.get(&touched_at_key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object touched_at"))?;
		if touched_at_value.is_none() {
			return Err(tg::error!("failed to find the object"));
		}

		// Use atomic max to update touched_at.
		let touched_at_bytes = touched_at.to_be_bytes();
		txn.atomic_op(&touched_at_key, &touched_at_bytes, MutationType::Max);

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(())
	}

	pub async fn touch_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Check if the process exists by looking for the touched_at field.
		let touched_at_key = Key::ProcessField {
			id,
			field: ProcessKind::TouchedAt,
		}
		.pack_to_vec();
		let touched_at_value = txn
			.get(&touched_at_key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process touched_at"))?;
		if touched_at_value.is_none() {
			return Err(tg::error!("failed to find the process"));
		}

		// Use atomic max to update touched_at.
		let touched_at_bytes = touched_at.to_be_bytes();
		txn.atomic_op(&touched_at_key, &touched_at_bytes, MutationType::Max);

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		Ok(())
	}
}
