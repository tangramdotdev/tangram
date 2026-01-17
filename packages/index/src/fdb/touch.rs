use {
	super::{Index, Key, ObjectValue, ProcessValue},
	crate::{ObjectStored, ProcessStored},
	foundationdb_tuple::TuplePack as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		let key = Key::Object(id).pack_to_vec();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Get the current value.
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;
		let Some(value) = value else {
			return Ok(None);
		};
		let mut value = tangram_serialize::from_slice::<ObjectValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;

		// Update touched_at if the new value is greater.
		if touched_at > value.touched_at {
			value.touched_at = touched_at;
			let serialized = tangram_serialize::to_vec(&value)
				.map_err(|source| tg::error!(!source, "failed to serialize object value"))?;
			txn.set(&key, &serialized);
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(Some((value.stored, value.metadata)))
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

		let values = futures::future::try_join_all(ids.iter().map(|id| async {
			let key = Key::Object(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?;
			let Some(value) = value else {
				return Ok::<_, tg::Error>(None);
			};
			let value = tangram_serialize::from_slice::<ObjectValue>(&value)
				.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;
			Ok(Some(value))
		}))
		.await?;

		let mut results = Vec::with_capacity(ids.len());
		let mut needs_commit = false;
		for (id, value) in ids.iter().zip(values) {
			let Some(mut value) = value else {
				results.push(None);
				continue;
			};

			// Update touched_at if the new value is greater.
			if touched_at > value.touched_at {
				value.touched_at = touched_at;
				let serialized = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize object value"))?;
				let key = Key::Object(id).pack_to_vec();
				txn.set(&key, &serialized);
				needs_commit = true;
			}

			results.push(Some((value.stored, value.metadata)));
		}

		if needs_commit {
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(results)
	}

	pub async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
		let key = Key::Process(id).pack_to_vec();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Get the current value.
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;
		let Some(value) = value else {
			return Ok(None);
		};
		let mut value = tangram_serialize::from_slice::<ProcessValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize process value"))?;

		// Update touched_at if the new value is greater.
		if touched_at > value.touched_at {
			value.touched_at = touched_at;
			let serialized = tangram_serialize::to_vec(&value)
				.map_err(|source| tg::error!(!source, "failed to serialize process value"))?;
			txn.set(&key, &serialized);
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(Some((value.stored, value.metadata)))
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

		let values = futures::future::try_join_all(ids.iter().map(|id| async {
			let key = Key::Process(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process"))?;
			let Some(value) = value else {
				return Ok::<_, tg::Error>(None);
			};
			let value = tangram_serialize::from_slice::<ProcessValue>(&value)
				.map_err(|source| tg::error!(!source, "failed to deserialize process value"))?;
			Ok(Some(value))
		}))
		.await?;

		let mut results = Vec::with_capacity(ids.len());
		let mut needs_commit = false;
		for (id, value) in ids.iter().zip(values) {
			let Some(mut value) = value else {
				results.push(None);
				continue;
			};

			// Update touched_at if the new value is greater.
			if touched_at > value.touched_at {
				value.touched_at = touched_at;
				let serialized = tangram_serialize::to_vec(&value)
					.map_err(|source| tg::error!(!source, "failed to serialize process value"))?;
				let key = Key::Process(id).pack_to_vec();
				txn.set(&key, &serialized);
				needs_commit = true;
			}

			results.push(Some((value.stored, value.metadata)));
		}

		if needs_commit {
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(results)
	}

	pub async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let key = Key::Object(id).pack_to_vec();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Get the current value.
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;
		let Some(value) = value else {
			return Err(tg::error!("failed to find the object"));
		};
		let mut value = tangram_serialize::from_slice::<ObjectValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;

		// Update touched_at if the new value is greater.
		if touched_at > value.touched_at {
			value.touched_at = touched_at;
			let serialized = tangram_serialize::to_vec(&value)
				.map_err(|source| tg::error!(!source, "failed to serialize object value"))?;
			txn.set(&key, &serialized);
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(())
	}

	pub async fn touch_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let key = Key::Process(id).pack_to_vec();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Get the current value.
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;
		let Some(value) = value else {
			return Err(tg::error!("failed to find the process"));
		};
		let mut value = tangram_serialize::from_slice::<ProcessValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize process value"))?;

		// Update touched_at if the new value is greater.
		if touched_at > value.touched_at {
			value.touched_at = touched_at;
			let serialized = tangram_serialize::to_vec(&value)
				.map_err(|source| tg::error!(!source, "failed to serialize process value"))?;
			txn.set(&key, &serialized);
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(())
	}
}
