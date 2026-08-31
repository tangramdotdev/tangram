use {super::Store, crate::object, std::borrow::Cow, tangram_client::prelude::*};

impl Store {
	pub fn delete_object_cache_entry(&self, arg: object::cache::delete::Arg) {
		let mut state = self.state();
		let entry = arg.entry;
		if state
			.objects
			.get(&entry.id)
			.is_some_and(|object| object.object.put == entry.put)
		{
			state.objects.remove(&entry.id);
		}
		state.object_cache.remove(&(entry.partition, entry.cache));
	}

	#[must_use]
	pub fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> Vec<object::cache::Entry> {
		let state = self.state();
		state
			.object_cache
			.iter()
			.filter(|((partition, _), _)| *partition == arg.partition)
			.take(arg.batch_size)
			.map(|((partition, cache), (id, put))| object::cache::Entry {
				cache: *cache,
				id: id.clone(),
				partition: *partition,
				put: *put,
			})
			.collect()
	}

	pub fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		self.state()
			.object_cache
			.insert((arg.partition, arg.cache), (arg.id, arg.put));

		Ok(())
	}

	pub fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let object = arg.object;
		let mut state = self.state();
		state
			.object_cache
			.insert((arg.partition, arg.cache), (object.id.clone(), object.put));
		let previous = state.objects.get(&object.id);
		if previous.is_some_and(|previous| previous.object.put > object.put) {
			return Ok(());
		}
		let value = object::Object {
			bytes: object.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: object.checkout_pointer,
			length: object.length,
			put: object.put,
		};
		let value = super::Object { object: value };
		state.objects.insert(object.id, value);

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes};

	fn object(id: tg::object::Id, put: u8) -> object::put::Arg {
		object::put::Arg {
			bytes: Some(Bytes::from_static(b"object")),
			checkout_pointer: None,
			id,
			length: None,
			put: [put; 16],
		}
	}

	#[test]
	fn a_stale_entry_does_not_delete_a_newer_object() {
		let store = Store::new();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		store.put_object(object(id.clone(), 10)).unwrap();
		store
			.put_object_cache_entry_with_object(object::cache::put::object::Arg {
				cache: [10; 16],
				object: object(id.clone(), 1),
				partition: 2,
			})
			.unwrap();
		let entries = store.get_object_cache_entries(object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 2,
		});
		assert_eq!(entries.len(), 1);
		store.delete_object_cache_entry(object::cache::delete::Arg {
			entry: entries[0].clone(),
		});
		let output = store.try_get_object_sync(&object::get::Arg {
			id: id.clone(),
			put: None,
		});
		assert_eq!(output.object.unwrap().put, [10; 16]);

		store
			.put_object_cache_entry_with_object(object::cache::put::object::Arg {
				cache: [11; 16],
				object: object(id.clone(), 11),
				partition: 3,
			})
			.unwrap();
		let output = store.try_get_object_sync(&object::get::Arg {
			id: id.clone(),
			put: None,
		});
		assert_eq!(output.object.unwrap().put, [11; 16]);
		let entries = store.get_object_cache_entries(object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 3,
		});
		store.delete_object_cache_entry(object::cache::delete::Arg {
			entry: entries[0].clone(),
		});
		let output = store.try_get_object_sync(&object::get::Arg { id, put: None });
		assert!(output.object.is_none());
	}

	#[test]
	fn an_archive_entry_deletes_the_stored_object() {
		let store = Store::new();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		store.put_object(object(id.clone(), 10)).unwrap();
		store
			.put_object_cache_entry(object::cache::put::Arg {
				cache: [20; 16],
				id: id.clone(),
				partition: 2,
				put: [10; 16],
			})
			.unwrap();
		let entries = store.get_object_cache_entries(object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 2,
		});
		store.delete_object_cache_entry(object::cache::delete::Arg {
			entry: entries[0].clone(),
		});
		let output = store.try_get_object_sync(&object::get::Arg { id, put: None });
		assert!(output.object.is_none());
	}
}
