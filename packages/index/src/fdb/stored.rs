use {
	super::{Index, Key, ObjectValue, ProcessValue},
	crate::{ObjectStored, ProcessStored},
	foundationdb_tuple::TuplePack as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_object_stored(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<ObjectStored>> {
		let key = Key::Object(id).pack_to_vec();
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;
		let Some(value) = value else {
			return Ok(None);
		};
		let value = tangram_serialize::from_slice::<ObjectValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;
		Ok(Some(value.stored))
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
			let key = Key::Object(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?;
			let Some(value) = value else {
				return Ok(None);
			};
			let value = tangram_serialize::from_slice::<ObjectValue>(&value)
				.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;
			Ok::<_, tg::Error>(Some(value.stored))
		}))
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		let key = Key::Object(id).pack_to_vec();
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;
		let Some(value) = value else {
			return Ok(None);
		};
		let value = tangram_serialize::from_slice::<ObjectValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;
		Ok(Some((value.stored, value.metadata)))
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
			let key = Key::Object(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?;
			let Some(value) = value else {
				return Ok(None);
			};
			let value = tangram_serialize::from_slice::<ObjectValue>(&value)
				.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;
			Ok::<_, tg::Error>(Some((value.stored, value.metadata)))
		}))
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<ProcessStored>> {
		let key = Key::Process(id).pack_to_vec();
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;
		let Some(value) = value else {
			return Ok(None);
		};
		let value = tangram_serialize::from_slice::<ProcessValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize process value"))?;
		Ok(Some(value.stored))
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
			let key = Key::Process(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process"))?;
			let Some(value) = value else {
				return Ok(None);
			};
			let value = tangram_serialize::from_slice::<ProcessValue>(&value)
				.map_err(|source| tg::error!(!source, "failed to deserialize process value"))?;
			Ok::<_, tg::Error>(Some(value.stored))
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
			let key = Key::Process(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process"))?;
			let Some(value) = value else {
				return Ok(None);
			};
			let value = tangram_serialize::from_slice::<ProcessValue>(&value)
				.map_err(|source| tg::error!(!source, "failed to deserialize process value"))?;
			Ok::<_, tg::Error>(Some((value.stored, value.metadata)))
		}))
		.await?;

		Ok(outputs)
	}
}
