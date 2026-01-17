use {
	super::{Index, Key, ObjectValue, ProcessValue},
	foundationdb_tuple::TuplePack as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
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
		Ok(Some(value.metadata))
	}

	pub async fn try_get_object_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
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
			Ok::<_, tg::Error>(Some(value.metadata))
		}))
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
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
		Ok(Some(value.metadata))
	}

	pub async fn try_get_process_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
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
			Ok::<_, tg::Error>(Some(value.metadata))
		}))
		.await?;

		Ok(outputs)
	}
}
