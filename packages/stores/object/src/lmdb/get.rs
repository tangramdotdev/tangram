use {
	super::{Db, Key, Store},
	crate::{Object, TryGetArg, TryGetBatchArg, TryGetOutput},
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		let request = crate::read::Request::TryGet(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGet(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		if arg.ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetBatch(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub fn try_get_sync(&self, arg: &TryGetArg) -> tg::Result<TryGetOutput> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_with_transaction(&transaction, arg)
	}

	pub fn try_get_batch_sync(&self, arg: &TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::try_get_batch_with_transaction(&self.db, &transaction, arg)
	}

	pub fn try_get_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_data_with_transaction(&transaction, id)
	}

	pub fn try_get_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		arg: &TryGetArg,
	) -> tg::Result<TryGetOutput> {
		let object = Self::try_get_object_with_transaction(&self.db, transaction, &arg.id)?;
		Ok(TryGetOutput { object })
	}

	pub(super) fn try_get_batch_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &TryGetBatchArg,
	) -> tg::Result<Vec<TryGetOutput>> {
		let mut outputs = Vec::with_capacity(arg.ids.len());
		for id in &arg.ids {
			let object = Self::try_get_object_with_transaction(db, transaction, id)?;
			outputs.push(TryGetOutput { object });
		}

		Ok(outputs)
	}

	pub(super) fn try_get_object_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object<'static>>> {
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();
		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		Ok(Some(value))
	}

	pub fn try_get_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let kind = id.kind();
		let Some(value) = Self::try_get_object_with_transaction(&self.db, transaction, id)? else {
			return Ok(None);
		};
		let Some(bytes) = value.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, &*bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object data"))?;
		Ok(Some((size, data)))
	}
}
