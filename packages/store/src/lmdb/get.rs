use {
	super::{Db, Key, Store, object as lmdb_object},
	crate::object,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn try_get_object(
		&self,
		arg: object::get::Arg,
	) -> tg::Result<object::get::Output> {
		let request = crate::read::Request::TryGetObject(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetObject(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		if arg.ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetObjectBatch(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetObjectBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub fn try_get_object_sync(&self, arg: &object::get::Arg) -> tg::Result<object::get::Output> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_object_with_transaction(&transaction, arg)
	}

	pub fn try_get_object_batch_sync(
		&self,
		arg: &object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::try_get_object_batch_with_transaction(&self.db, &transaction, arg)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_object_data_with_transaction(&transaction, id)
	}

	pub fn try_get_object_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		arg: &object::get::Arg,
	) -> tg::Result<object::get::Output> {
		let object = Self::try_get_object_inner_with_transaction(&self.db, transaction, &arg.id)?;
		Ok(object::get::Output { object })
	}

	pub(super) fn try_get_object_batch_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		let mut outputs = Vec::with_capacity(arg.ids.len());
		for id in &arg.ids {
			let object = Self::try_get_object_inner_with_transaction(db, transaction, id)?;
			outputs.push(object::get::Output { object });
		}

		Ok(outputs)
	}

	pub(super) fn try_get_object_inner_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<object::Object<'static>>> {
		let key = Key::Object(lmdb_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();
		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = object::Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		Ok(Some(value))
	}

	pub fn try_get_object_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let kind = id.kind();
		let Some(value) = Self::try_get_object_inner_with_transaction(&self.db, transaction, id)?
		else {
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
