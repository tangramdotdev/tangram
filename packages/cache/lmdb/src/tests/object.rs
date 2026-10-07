use {
	crate::Cache, bytes::Bytes, std::path::Path, tangram_cache::object, tangram_client::prelude::*,
};

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
	let config = crate::Config {
		map_size: 1024 * 1024 * 10,
		path: path.join("test.lmdb"),
		posix_sem_prefix: None,
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 8_000,
	};
	Cache::new(&config).unwrap()
}

#[tokio::test]
async fn entries_are_ordered_and_persistent() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let first = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"first"));
	let second = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"second"));
	{
		let cache = cache(temp.path());
		cache
			.put_object_cache_entry(object::cache::put::Arg {
				cache: [20; 16],
				id: second.clone(),
				partition: 2,
				put: [2; 16],
			})
			.await
			.unwrap();
		cache
			.put_object_cache_entry(object::cache::put::Arg {
				cache: [10; 16],
				id: first.clone(),
				partition: 2,
				put: [1; 16],
			})
			.await
			.unwrap();
	}

	let cache = cache(temp.path());
	let entries = cache
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
	let cache = cache(temp.path());
	let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
	cache.put_object(object(id.clone(), 10)).await.unwrap();
	cache
		.put_object_cache_entry_with_object(object::cache::put::object::Arg {
			cache: [10; 16],
			object: object(id.clone(), 1),
			partition: 2,
		})
		.await
		.unwrap();
	let entries = cache
		.get_object_cache_entries(object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 2,
		})
		.await
		.unwrap();
	cache
		.delete_object_cache_entry(object::cache::delete::Arg {
			entry: entries[0].clone(),
		})
		.await
		.unwrap();
	let arg = object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let output = cache.try_get_object(arg).await.unwrap();
	assert_eq!(output.object.unwrap().put, [10; 16]);

	cache
		.put_object_cache_entry_with_object(object::cache::put::object::Arg {
			cache: [11; 16],
			object: object(id.clone(), 11),
			partition: 3,
		})
		.await
		.unwrap();
	let arg = object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let output = cache.try_get_object(arg).await.unwrap();
	assert_eq!(output.object.unwrap().put, [11; 16]);
	let entries = cache
		.get_object_cache_entries(object::cache::get::Arg {
			batch_size: usize::MAX,
			partition: 3,
		})
		.await
		.unwrap();
	cache
		.delete_object_cache_entry(object::cache::delete::Arg {
			entry: entries[0].clone(),
		})
		.await
		.unwrap();
	let output = cache
		.try_get_object(object::get::Arg {
			bytes: true,
			id,
			put: None,
		})
		.await
		.unwrap();
	assert!(output.object.is_none());
}
