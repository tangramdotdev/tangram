use {crate::Cache, bytes::Bytes, tangram_cache::object, tangram_client::prelude::*};

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
	let cache = Cache::new();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
	cache.put_object(object(id.clone(), 10)).unwrap();
	cache
		.put_object_cache_entry_with_object(object::cache::put::object::Arg {
			cache: [10; 16],
			object: object(id.clone(), 1),
			partition: 2,
		})
		.unwrap();
	let entries = cache.get_object_cache_entries(object::cache::get::Arg {
		batch_size: usize::MAX,
		partition: 2,
	});
	assert_eq!(entries.len(), 1);
	cache.delete_object_cache_entry(object::cache::delete::Arg {
		entry: entries[0].clone(),
	});
	let output = cache.try_get_object_sync(&object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	});
	assert_eq!(output.object.unwrap().put, [10; 16]);

	cache
		.put_object_cache_entry_with_object(object::cache::put::object::Arg {
			cache: [11; 16],
			object: object(id.clone(), 11),
			partition: 3,
		})
		.unwrap();
	let output = cache.try_get_object_sync(&object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	});
	assert_eq!(output.object.unwrap().put, [11; 16]);
	let entries = cache.get_object_cache_entries(object::cache::get::Arg {
		batch_size: usize::MAX,
		partition: 3,
	});
	cache.delete_object_cache_entry(object::cache::delete::Arg {
		entry: entries[0].clone(),
	});
	let output = cache.try_get_object_sync(&object::get::Arg {
		bytes: true,
		id,
		put: None,
	});
	assert!(output.object.is_none());
}

#[test]
fn an_archive_entry_deletes_the_stored_object() {
	let cache = Cache::new();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
	cache.put_object(object(id.clone(), 10)).unwrap();
	cache
		.put_object_cache_entry(object::cache::put::Arg {
			cache: [20; 16],
			id: id.clone(),
			partition: 2,
			put: [10; 16],
		})
		.unwrap();
	let entries = cache.get_object_cache_entries(object::cache::get::Arg {
		batch_size: usize::MAX,
		partition: 2,
	});
	cache.delete_object_cache_entry(object::cache::delete::Arg {
		entry: entries[0].clone(),
	});
	let output = cache.try_get_object_sync(&object::get::Arg {
		bytes: true,
		id,
		put: None,
	});
	assert!(output.object.is_none());
}
