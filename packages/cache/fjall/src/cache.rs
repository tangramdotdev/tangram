use {
	super::{Cache, Key, Kind, object as fjall_object},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	num_traits::ToPrimitive as _,
	std::borrow::Cow,
	tangram_cache::object,
	tangram_client::prelude::*,
};

impl Cache {
	pub async fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::DeleteObjectCacheEntry(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub(super) fn delete_object_cache_entry_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let object_key = Key::Object(fjall_object::Key::Object(&entry.id)).pack_to_vec();
		let value = transaction
			.get(&object_key)
			.map_err(|error| tg::error!(!error, id = %entry.id, "failed to get the object"))?
			.map(|bytes| fjall_object::Value::deserialize(&bytes))
			.transpose()
			.map_err(
				|error| tg::error!(!error, id = %entry.id, "failed to deserialize the object"),
			)?;
		if value.is_some_and(|value| value.object.put == entry.put) {
			transaction.delete(&object_key).map_err(
				|error| tg::error!(!error, id = %entry.id, "failed to delete the object"),
			)?;
		}
		let key = Key::ObjectCache(entry).pack_to_vec();
		transaction
			.delete(&key)
			.map_err(|error| tg::error!(!error, "failed to delete an object cache entry"))?;

		Ok(())
	}

	pub async fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		let request = crate::read::Request::GetObjectCacheEntries(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetObjectCacheEntries(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub(super) fn get_object_cache_entries_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		arg: &object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		let prefix = fdbt::pack(&(Kind::ObjectCache.to_i32().unwrap(), arg.partition));
		let entries = transaction.prefix_iter(&prefix);
		entries
			.take(arg.batch_size)
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to get an object cache entry"))?;
				let (_, partition, cache): (i32, u64, Vec<u8>) = fdbt::unpack(&key)
					.map_err(|error| tg::error!(!error, "failed to unpack an object cache key"))?;
				let (id, put): (Vec<u8>, Vec<u8>) = fdbt::unpack(&value).map_err(|error| {
					tg::error!(!error, "failed to unpack an object cache value")
				})?;
				let cache = cache
					.try_into()
					.map_err(|_| tg::error!("the object cache id is invalid"))?;
				let id = tg::object::Id::from_slice(&id)?;
				let put = put
					.try_into()
					.map_err(|_| tg::error!("the object cache put is invalid"))?;
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

	pub async fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		let request = super::request::Request::PutObjectCacheEntry(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub(super) fn put_object_cache_entry_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
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
		transaction
			.put(&key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		Ok(())
	}

	pub async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::PutObjectCacheEntryWithObject(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub(super) fn put_object_cache_entry_with_object_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
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
		transaction
			.put(&key, &entry_value)
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		let key = Key::Object(fjall_object::Key::Object(&object.id)).pack_to_vec();
		let previous = transaction
			.get(&key)
			.map_err(|error| tg::error!(!error, id = %object.id, "failed to get the object"))?
			.map(|bytes| fjall_object::Value::deserialize(&bytes))
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
		let value = fjall_object::Value::new(value).serialize()?;
		transaction
			.put(&key, &value)
			.map_err(|error| tg::error!(!error, id = %object.id, "failed to put the object"))?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::path::Path};

	#[tokio::test]
	async fn entries_are_ordered_and_persistent() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let first = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"first"));
		let second = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"second"));
		{
			let cache = cache(temp.path());
			let arg = object::cache::put::Arg {
				cache: [20; 16],
				id: second.clone(),
				partition: 2,
				put: [2; 16],
			};
			cache.put_object_cache_entry(arg).await.unwrap();
			let arg = object::cache::put::Arg {
				cache: [10; 16],
				id: first.clone(),
				partition: 2,
				put: [1; 16],
			};
			cache.put_object_cache_entry(arg).await.unwrap();
		}

		let cache = cache(temp.path());
		let arg = object::cache::get::Arg {
			batch_size: 1,
			partition: 2,
		};
		let entries = cache.get_object_cache_entries(arg).await.unwrap();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].id, first);
		assert_eq!(entries[0].cache, [10; 16]);
	}

	#[tokio::test]
	async fn a_stale_entry_does_not_delete_a_newer_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let cache = cache(temp.path());
		let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		cache.put_object(object(id.clone(), 10)).await.unwrap();
		let arg = object::cache::put::object::Arg {
			cache: [10; 16],
			object: object(id.clone(), 1),
			partition: 2,
		};
		cache.put_object_cache_entry_with_object(arg).await.unwrap();
		let arg = object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 2,
		};
		let entries = cache.get_object_cache_entries(arg).await.unwrap();
		let arg = object::cache::delete::Arg {
			entry: entries[0].clone(),
		};
		cache.delete_object_cache_entry(arg).await.unwrap();
		let arg = object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let output = cache.try_get_object(arg).await.unwrap();
		assert_eq!(output.object.unwrap().put, [10; 16]);

		let arg = object::cache::put::object::Arg {
			cache: [11; 16],
			object: object(id.clone(), 11),
			partition: 3,
		};
		cache.put_object_cache_entry_with_object(arg).await.unwrap();
		let arg = object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let output = cache.try_get_object(arg).await.unwrap();
		assert_eq!(output.object.unwrap().put, [11; 16]);
		let arg = object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 3,
		};
		let entries = cache.get_object_cache_entries(arg).await.unwrap();
		let arg = object::cache::delete::Arg {
			entry: entries[0].clone(),
		};
		cache.delete_object_cache_entry(arg).await.unwrap();
		let arg = object::get::Arg {
			bytes: true,
			id,
			put: None,
		};
		let output = cache.try_get_object(arg).await.unwrap();
		assert!(output.object.is_none());
	}

	fn object(id: tg::object::Id, put: u8) -> object::put::Arg {
		object::put::Arg {
			bytes: Some(Bytes::from_static(b"object")),
			checkout_pointer: None,
			id,
			length: None,
			put: [put; 16],
		}
	}

	fn cache(path: &Path) -> Cache {
		let config = super::super::Config {
			path: path.join("test.fjall"),
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		Cache::new(&config).unwrap()
	}
}
