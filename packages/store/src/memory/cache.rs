use {super::Store, crate::object, std::borrow::Cow, tangram_client::prelude::*};

impl Store {
	pub fn delete_object_cache_entry(&self, arg: object::cache::delete::Arg) {
		let mut state = self.state();
		let entry = arg.entry;
		if state
			.objects
			.get(&entry.id)
			.is_some_and(|object| object.timestamp <= entry.cached_at)
		{
			state.objects.remove(&entry.id);
		}
		state
			.object_cache
			.remove(&(entry.partition, entry.cached_at, entry.id));
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
			.filter(|(partition, _, _)| *partition == arg.partition)
			.take(arg.batch_size)
			.map(|(partition, cached_at, id)| object::cache::Entry {
				cached_at: *cached_at,
				id: id.clone(),
				partition: *partition,
			})
			.collect()
	}

	pub fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		let cached_at = object::cache::stored_at_timestamp(arg.stored_at)?;
		self.state()
			.object_cache
			.insert((arg.partition, cached_at, arg.id));

		Ok(())
	}

	pub fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let cached_at = object::cache::cached_at_timestamp(arg.cached_at)?;
		let object = arg.object;
		let mut state = self.state();
		state
			.object_cache
			.insert((arg.partition, cached_at, object.id.clone()));
		let previous = state.objects.get(&object.id);
		if previous.is_some_and(|object| object.timestamp > cached_at) {
			return Ok(());
		}
		let stored_at = previous.map_or(object.stored_at, |previous| {
			previous.object.stored_at.max(object.stored_at)
		});
		let value = object::Object {
			bytes: object.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			checkout_pointer: object.checkout_pointer,
			length: object.length,
			stored_at,
		};
		let value = super::Object {
			object: value,
			timestamp: cached_at,
		};
		state.objects.insert(object.id, value);

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes};

	fn object(id: tg::object::Id, stored_at: i64) -> object::put::Arg {
		object::put::Arg {
			bytes: Some(Bytes::from_static(b"object")),
			checkout_pointer: None,
			id,
			length: None,
			stored_at,
		}
	}

	#[test]
	fn a_stale_entry_does_not_delete_a_newer_object() {
		let store = Store::new();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		store.put_object(object(id.clone(), 10)).unwrap();
		store
			.put_object_cache_entry_with_object(object::cache::put::object::Arg {
				cached_at: 10,
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
		let output = store.try_get_object_sync(&object::get::Arg { id: id.clone() });
		assert_eq!(output.object.unwrap().stored_at, 10);

		store
			.put_object_cache_entry_with_object(object::cache::put::object::Arg {
				cached_at: 11,
				object: object(id.clone(), 1),
				partition: 3,
			})
			.unwrap();
		let output = store.try_get_object_sync(&object::get::Arg { id: id.clone() });
		assert_eq!(output.object.unwrap().stored_at, 10);
		let entries = store.get_object_cache_entries(object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 3,
		});
		store.delete_object_cache_entry(object::cache::delete::Arg {
			entry: entries[0].clone(),
		});
		let output = store.try_get_object_sync(&object::get::Arg { id });
		assert!(output.object.is_none());
	}

	#[test]
	fn an_archive_entry_deletes_the_stored_object() {
		let store = Store::new();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		store.put_object(object(id.clone(), 10)).unwrap();
		store
			.put_object_cache_entry(object::cache::put::Arg {
				id: id.clone(),
				partition: 2,
				stored_at: 10,
			})
			.unwrap();
		let entries = store.get_object_cache_entries(object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 2,
		});
		store.delete_object_cache_entry(object::cache::delete::Arg {
			entry: entries[0].clone(),
		});
		let output = store.try_get_object_sync(&object::get::Arg { id });
		assert!(output.object.is_none());
	}
}
