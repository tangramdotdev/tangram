use {
	crate::{
		Cache, Db, Key as CacheKey, Kind,
		object::{self as lmdb_object, delete::Request, put::Request as PutRequest},
	},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_cache::object,
	tangram_client::prelude::*,
};

mod key;

pub(super) use key::Key;

pub(super) mod delete;
pub(super) mod put;

#[derive(tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(super) struct Value<'a> {
	#[tangram_serialize(id = 0)]
	pub object: tangram_cache::object::Object<'a>,
}

impl Cache {
	pub(super) async fn contains_object(&self, arg: object::contains::Arg) -> tg::Result<bool> {
		let arg = object::get::Arg {
			bytes: false,
			id: arg.id,
			put: Some(arg.put),
		};
		let output = self.try_get_object(arg).await?;

		Ok(output.object.is_some())
	}

	pub async fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let request = crate::request::Request::DeleteObjectCacheEntry(arg);

		self.send_write_request(request).await
	}

	pub(super) async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let request = crate::request::Request::DeleteObject(Request {
			id: arg.id,
			put: arg.put,
		});

		self.send_write_request(request).await
	}

	pub(super) async fn delete_object_batch(
		&self,
		args: Vec<object::delete::Arg>,
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = crate::request::Request::DeleteObjectBatch(
			args.into_iter()
				.map(|arg| Request {
					id: arg.id,
					put: arg.put,
				})
				.collect(),
		);

		self.send_write_request(request).await
	}

	pub async fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		let request = crate::read::Request::GetObjectCacheEntries(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetObjectCacheEntries(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutObjectCacheEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let request = crate::request::Request::PutObjectCacheEntryWithObject(arg);

		self.send_write_request(request).await
	}

	pub(super) async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutObject(PutRequest {
			bytes: arg.bytes,
			checkout_pointer: arg.checkout_pointer,
			id: arg.id,
			length: arg.length,
			put: arg.put,
		});

		self.send_write_request(request).await
	}

	pub(super) async fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = crate::request::Request::PutObjectBatch(
			args.into_iter()
				.map(|arg| PutRequest {
					bytes: arg.bytes,
					checkout_pointer: arg.checkout_pointer,
					id: arg.id,
					length: arg.length,
					put: arg.put,
				})
				.collect(),
		);

		self.send_write_request(request).await
	}

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

	pub(super) fn delete_object_cache_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let object_key = CacheKey::Object(lmdb_object::Key::Object(&entry.id)).pack_to_vec();
		let value = db
			.get(transaction, &object_key)
			.map_err(|error| tg::error!(!error, id = %entry.id, "failed to get the object"))?
			.map(lmdb_object::Value::deserialize)
			.transpose()
			.map_err(
				|error| tg::error!(!error, id = %entry.id, "failed to deserialize the object"),
			)?;
		if value.is_some_and(|value| value.object.put == entry.put) {
			db.delete(transaction, &object_key).map_err(
				|error| tg::error!(!error, id = %entry.id, "failed to delete the object"),
			)?;
		}
		let key = CacheKey::ObjectCache(entry).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete an object cache entry"))?;

		Ok(())
	}

	pub(super) fn get_object_cache_entries_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		let prefix = fdbt::pack(&(Kind::ObjectCache.to_i32().unwrap(), arg.partition));
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the object cache"))?;
		entries
			.take(arg.batch_size)
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to get an object cache entry"))?;
				let (_, partition, cache): (i32, u64, Vec<u8>) = fdbt::unpack(key)
					.map_err(|error| tg::error!(!error, "failed to unpack an object cache key"))?;
				let (id, put): (Vec<u8>, Vec<u8>) = fdbt::unpack(value).map_err(|error| {
					tg::error!(!error, "failed to unpack an object cache value")
				})?;
				let cache = cache
					.try_into()
					.map_err(|_| tg::error!("invalid object cache id"))?;
				let id = tg::object::Id::from_slice(&id)?;
				let put = put
					.try_into()
					.map_err(|_| tg::error!("invalid object cache put"))?;
				let entry = object::cache::Entry {
					cache,
					id,
					partition,
					put,
				};

				Ok(entry)
			})
			.collect()
	}

	pub(super) fn put_object_cache_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: object::cache::put::Arg,
	) -> tg::Result<()> {
		let entry = object::cache::Entry {
			cache: arg.cache,
			id: arg.id,
			partition: arg.partition,
			put: arg.put,
		};
		let id = entry.id.to_bytes();
		let value = fdbt::pack(&(id.as_ref(), entry.put.as_slice()));
		let key = CacheKey::ObjectCache(entry).pack_to_vec();
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		Ok(())
	}

	pub(super) fn put_object_cache_entry_with_object_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let object = arg.object;
		let entry = object::cache::Entry {
			cache: arg.cache,
			id: object.id.clone(),
			partition: arg.partition,
			put: object.put,
		};
		let id = entry.id.to_bytes();
		let entry_value = fdbt::pack(&(id.as_ref(), entry.put.as_slice()));
		let key = CacheKey::ObjectCache(entry).pack_to_vec();
		db.put(transaction, &key, &entry_value)
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		let key = CacheKey::Object(lmdb_object::Key::Object(&object.id)).pack_to_vec();
		let previous = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %object.id, "failed to get the object"))?
			.map(lmdb_object::Value::deserialize)
			.transpose()
			.map_err(
				|error| tg::error!(!error, id = %object.id, "failed to deserialize the object"),
			)?;
		if previous
			.as_ref()
			.is_some_and(|previous| previous.object.put > object.put)
		{
			return Ok(());
		}
		let value = object::Object {
			bytes: object.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: object.checkout_pointer,
			length: object.length,
			put: object.put,
		};
		let value = lmdb_object::Value::new(value).serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, id = %object.id, "failed to put the object"))?;

		Ok(())
	}

	pub fn delete_object_sync(&self, arg: object::delete::Arg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = Request {
			id: arg.id,
			put: arg.put,
		};
		Self::delete_inner_with_transaction(&self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_object_batch_sync(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = Request {
				id: arg.id,
				put: arg.put,
			};
			Self::delete_inner_with_transaction(&self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	pub(super) fn delete_inner_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Request,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = CacheKey::Object(lmdb_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();

		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(());
		};
		let value = lmdb_object::Value::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		if value.object.put == request.put {
			db.delete(transaction, &key_bytes)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the object"))?;
		}

		Ok(())
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
		Self::try_get_object_with_arg_with_transaction(&self.db, transaction, arg)
	}

	pub(super) fn try_get_object_with_arg_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &object::get::Arg,
	) -> tg::Result<object::get::Output> {
		let object =
			Self::try_get_object_with_bytes_with_transaction(db, transaction, &arg.id, arg.bytes)?;
		let object = object.filter(|object| arg.put.is_none_or(|put| object.put == put));
		Ok(object::get::Output { object })
	}

	fn try_get_object_with_bytes_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		include_bytes: bool,
	) -> tg::Result<Option<object::Object<'static>>> {
		let key = CacheKey::Object(lmdb_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();
		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = lmdb_object::Value::deserialize_with_bytes(bytes, include_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		Ok(Some(value.object))
	}

	pub(super) fn try_get_object_batch_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		let mut outputs = Vec::with_capacity(arg.ids.len());
		for id in &arg.ids {
			let object =
				Self::try_get_object_with_bytes_with_transaction(db, transaction, id, arg.bytes)?;
			outputs.push(object::get::Output { object });
		}

		Ok(outputs)
	}

	pub(super) fn try_get_object_inner_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<object::Object<'static>>> {
		Self::try_get_object_with_bytes_with_transaction(db, transaction, id, true)
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

	pub fn put_object_sync(&self, arg: object::put::Arg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = PutRequest {
			bytes: arg.bytes,
			checkout_pointer: arg.checkout_pointer,
			id: arg.id,
			length: arg.length,
			put: arg.put,
		};
		Self::put_inner_with_transaction(&self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_object_batch_sync(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = PutRequest {
				bytes: arg.bytes,
				checkout_pointer: arg.checkout_pointer,
				id: arg.id,
				length: arg.length,
				put: arg.put,
			};
			Self::put_inner_with_transaction(&self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(super) fn put_inner_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: PutRequest,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = CacheKey::Object(lmdb_object::Key::Object(id));
		let key_bytes = key.pack_to_vec();
		let previous = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.map(crate::object::Value::deserialize)
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		if previous.is_some_and(|object| object.object.put > request.put) {
			return Ok(());
		}

		let value = object::Object {
			bytes: request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: request.checkout_pointer,
			length: request.length,
			put: request.put,
		};
		let value = crate::object::Value::new(value);
		let value_bytes = value.serialize()?;
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		Ok(())
	}
}

impl Value<'_> {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		let mut bytes = vec![0];
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the object value"))?;

		Ok(bytes)
	}
}

impl Value<'static> {
	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		Self::deserialize_with_bytes(bytes, true)
	}

	pub fn deserialize_with_bytes(bytes: &[u8], include_bytes: bool) -> tg::Result<Self> {
		let Some((&format, bytes)) = bytes.split_first() else {
			return Err(tg::error!("empty object value data"));
		};
		if format != 0 {
			return Err(tg::error!("invalid object value format"));
		}
		let mut value: Value<'_> = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the object value"))?;
		if !include_bytes {
			value.object.bytes = None;
		}
		let object = value.object.into_static();

		Ok(Self { object })
	}
}

impl Value<'_> {
	pub fn new(object: tangram_cache::object::Object<'_>) -> Value<'static> {
		let object = tangram_cache::object::Object {
			bytes: object.bytes.map(|bytes| Cow::Owned(bytes.into_owned())),
			checkout_pointer: object.checkout_pointer,
			length: object.length,
			put: object.put,
		};

		Value { object }
	}
}

impl tangram_cache::object::Cache for Cache {
	async fn contains_object(&self, arg: tangram_cache::object::contains::Arg) -> tg::Result<bool> {
		self.contains_object(arg).await
	}

	async fn delete_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_cache_entry(arg).await
	}

	async fn delete_object(&self, arg: tangram_cache::object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await
	}

	async fn delete_object_batch(
		&self,
		args: Vec<tangram_cache::object::delete::Arg>,
	) -> tg::Result<()> {
		self.delete_object_batch(args).await
	}

	async fn get_object_cache_entries(
		&self,
		arg: tangram_cache::object::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::object::cache::Entry>> {
		self.get_object_cache_entries(arg).await
	}

	async fn put_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry(arg).await
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: tangram_cache::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg).await
	}

	async fn put_object(&self, arg: tangram_cache::object::put::Arg) -> tg::Result<()> {
		self.put_object(arg).await
	}

	async fn put_object_batch(&self, args: Vec<tangram_cache::object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args).await
	}

	async fn try_get_object(
		&self,
		arg: tangram_cache::object::get::Arg,
	) -> tg::Result<tangram_cache::object::get::Output> {
		self.try_get_object(arg).await
	}

	async fn try_get_object_batch(
		&self,
		arg: tangram_cache::object::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::object::get::Output>> {
		self.try_get_object_batch(arg).await
	}
}
