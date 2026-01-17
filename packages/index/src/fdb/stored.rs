use {
	super::{Index, Key, ObjectKind, ProcessKind},
	crate::{ObjectStored, ProcessStored},
	foundationdb_tuple::TuplePack as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_object_stored(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<ObjectStored>> {
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

		// Range scan for stored fields.
		let stored = Self::read_object_stored(&txn, id).await?;

		Ok(Some(stored))
	}

	pub async fn try_get_object_stored_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<ObjectStored>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let outputs = futures::future::try_join_all(ids.iter().map(|id| async {
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

			// Range scan for stored fields.
			let stored = Self::read_object_stored(&txn, id).await?;

			Ok::<_, tg::Error>(Some(stored))
		}))
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
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

		// Combined range scan for stored and metadata fields.
		let (stored, metadata) = Self::read_object_stored_and_metadata(&txn, id).await?;

		Ok(Some((stored, metadata)))
	}

	pub async fn try_get_object_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let outputs = futures::future::try_join_all(ids.iter().map(|id| async {
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

			// Combined range scan for stored and metadata fields.
			let (stored, metadata) = Self::read_object_stored_and_metadata(&txn, id).await?;

			Ok::<_, tg::Error>(Some((stored, metadata)))
		}))
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<ProcessStored>> {
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

		// Range scan for stored fields.
		let stored = Self::read_process_stored(&txn, id).await?;

		Ok(Some(stored))
	}

	pub async fn try_get_process_stored_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<ProcessStored>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let outputs = futures::future::try_join_all(ids.iter().map(|id| async {
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

			// Range scan for stored fields.
			let stored = Self::read_process_stored(&txn, id).await?;

			Ok::<_, tg::Error>(Some(stored))
		}))
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_process_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let outputs = futures::future::try_join_all(ids.iter().map(|id| async {
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

			// Combined range scan for stored and metadata fields.
			let (stored, metadata) = Self::read_process_stored_and_metadata(&txn, id).await?;

			Ok::<_, tg::Error>(Some((stored, metadata)))
		}))
		.await?;

		Ok(outputs)
	}
}
