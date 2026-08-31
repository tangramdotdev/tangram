use {
	super::{Db, Key, Kind, Store, object as lmdb_object},
	crate::object,
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::ToPrimitive as _,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::DeleteObjectCacheEntry(arg);

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
		let request = super::request::Request::PutObjectCacheEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::PutObjectCacheEntryWithObject(arg);

		self.send_write_request(request).await
	}

	pub(super) fn delete_object_cache_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let object_key = Key::Object(lmdb_object::Key::Object(&entry.id)).pack_to_vec();
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
		let key = Key::ObjectCache(entry).pack_to_vec();
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
		let key = Key::ObjectCache(entry).pack_to_vec();
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
		let key = Key::ObjectCache(entry).pack_to_vec();
		db.put(transaction, &key, &entry_value)
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		let key = Key::Object(lmdb_object::Key::Object(&object.id)).pack_to_vec();
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
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::path::Path};

	fn object(id: tg::object::Id, put: u8) -> object::put::Arg {
		object::put::Arg {
			bytes: Some(Bytes::from_static(b"object")),
			checkout_pointer: None,
			id,
			length: None,
			put: [put; 16],
		}
	}

	fn store(path: &Path) -> Store {
		let config = super::super::Config {
			map_size: 1024 * 1024 * 10,
			path: path.join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		Store::new(&config).unwrap()
	}

	#[tokio::test]
	async fn entries_are_ordered_and_persistent() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let first = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"first"));
		let second = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"second"));
		{
			let store = store(temp.path());
			store
				.put_object_cache_entry(object::cache::put::Arg {
					cache: [20; 16],
					id: second.clone(),
					partition: 2,
					put: [2; 16],
				})
				.await
				.unwrap();
			store
				.put_object_cache_entry(object::cache::put::Arg {
					cache: [10; 16],
					id: first.clone(),
					partition: 2,
					put: [1; 16],
				})
				.await
				.unwrap();
		}

		let store = store(temp.path());
		let entries = store
			.get_object_cache_entries(object::cache::get::Arg {
				batch_size: 1,
				partition: 2,
			})
			.await
			.unwrap();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].id, first);
		assert_eq!(entries[0].cache, [10; 16]);
	}

	#[tokio::test]
	async fn a_stale_entry_does_not_delete_a_newer_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let store = store(temp.path());
		let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		store.put_object(object(id.clone(), 10)).await.unwrap();
		store
			.put_object_cache_entry_with_object(object::cache::put::object::Arg {
				cache: [10; 16],
				object: object(id.clone(), 1),
				partition: 2,
			})
			.await
			.unwrap();
		let entries = store
			.get_object_cache_entries(object::cache::get::Arg {
				batch_size: usize::MAX,
				partition: 2,
			})
			.await
			.unwrap();
		store
			.delete_object_cache_entry(object::cache::delete::Arg {
				entry: entries[0].clone(),
			})
			.await
			.unwrap();
		let arg = object::get::Arg {
			id: id.clone(),
			put: None,
		};
		let output = store.try_get_object(arg).await.unwrap();
		assert_eq!(output.object.unwrap().put, [10; 16]);

		store
			.put_object_cache_entry_with_object(object::cache::put::object::Arg {
				cache: [11; 16],
				object: object(id.clone(), 11),
				partition: 3,
			})
			.await
			.unwrap();
		let arg = object::get::Arg {
			id: id.clone(),
			put: None,
		};
		let output = store.try_get_object(arg).await.unwrap();
		assert_eq!(output.object.unwrap().put, [11; 16]);
		let entries = store
			.get_object_cache_entries(object::cache::get::Arg {
				batch_size: usize::MAX,
				partition: 3,
			})
			.await
			.unwrap();
		store
			.delete_object_cache_entry(object::cache::delete::Arg {
				entry: entries[0].clone(),
			})
			.await
			.unwrap();
		let output = store
			.try_get_object(object::get::Arg { id, put: None })
			.await
			.unwrap();
		assert!(output.object.is_none());
	}
}
