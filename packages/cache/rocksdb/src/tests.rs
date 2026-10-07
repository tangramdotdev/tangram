use {super::*, bytes::Bytes, num::ToPrimitive as _, std::borrow::Cow};

mod object;
mod reader;

// An object put with bytes can be retrieved with the same bytes.
#[tokio::test]
async fn test_put_and_get_object() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = Config {
		path: temp.path().join("test.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 8_000,
	};
	let cache = Cache::new(&config).unwrap();

	// Create the object data and ID.
	let content = b"hello world";
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(content),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

	// Put the object.
	let arg = tangram_cache::object::put::Arg {
		bytes: Some(bytes.clone()),
		checkout_pointer: None,
		id: id.clone(),
		length: Some(content.len().to_u64().unwrap()),
		put: [1; 16],
	};
	cache.put_object(arg).await.unwrap();

	// Get the object.
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let result = cache.try_get_object(arg).await.unwrap().object;
	assert_eq!(
		result.and_then(|object| object.bytes),
		Some(Cow::Owned(bytes.to_vec()))
	);

	// Get the object without copying its bytes.
	let arg = tangram_cache::object::get::Arg {
		bytes: false,
		id: id.clone(),
		put: Some([1; 16]),
	};
	let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
	assert!(object.bytes.is_none());
	assert_eq!(object.put, [1; 16]);
	let arg = tangram_cache::object::get::batch::Arg {
		bytes: false,
		ids: vec![id.clone()],
	};
	let objects = cache.try_get_object_batch(arg).await.unwrap();
	let object = objects[0].object.as_ref().unwrap();
	assert!(object.bytes.is_none());
	assert_eq!(object.put, [1; 16]);

	// Get the object by its exact put.
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: Some([0; 16]),
	};
	let result = cache.try_get_object(arg).await.unwrap().object;
	assert!(result.is_none());
	let arg = tangram_cache::object::contains::Arg {
		id: id.clone(),
		put: [0; 16],
	};
	let contains = tangram_cache::object::Cache::contains_object(&cache, arg)
		.await
		.unwrap();
	assert!(!contains);
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: Some([1; 16]),
	};
	let result = cache.try_get_object(arg).await.unwrap().object;
	assert_eq!(result.unwrap().put, [1; 16]);
	let arg = tangram_cache::object::contains::Arg { id, put: [1; 16] };
	let contains = tangram_cache::object::Cache::contains_object(&cache, arg)
		.await
		.unwrap();
	assert!(contains);
}

// An object first put without bytes stores no bytes and a later put with bytes makes the bytes retrievable.
#[tokio::test]
async fn test_put_object_without_bytes_then_with_bytes() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = Config {
		path: temp.path().join("test.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 8_000,
	};
	let cache = Cache::new(&config).unwrap();

	// Create the object data and ID.
	let content = b"hello world";
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(content),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

	// Put without bytes first (should not cache anything).
	let arg = tangram_cache::object::put::Arg {
		bytes: None,
		checkout_pointer: None,
		id: id.clone(),
		length: None,
		put: [1; 16],
	};
	cache.put_object(arg).await.unwrap();

	// Verify object bytes do not exist (object may exist with bytes=None).
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let result = cache.try_get_object(arg).await.unwrap().object;
	assert!(
		result.is_none()
			|| result
				.as_ref()
				.and_then(|object| object.bytes.as_ref())
				.is_none()
	);

	// Put with bytes.
	let arg = tangram_cache::object::put::Arg {
		bytes: Some(bytes.clone()),
		checkout_pointer: None,
		id: id.clone(),
		length: Some(content.len().to_u64().unwrap()),
		put: [2; 16],
	};
	cache.put_object(arg).await.unwrap();

	// Verify object now exists.
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let result = cache.try_get_object(arg).await.unwrap().object;
	assert_eq!(
		result.and_then(|object| object.bytes),
		Some(Cow::Owned(bytes.to_vec()))
	);
}

// An object put and retrieved through the synchronous functions, as the server uses them, round-trips the bytes.
#[tokio::test]
async fn test_put_and_get_object_sync() {
	// This test mimics what the server does using sync functions.
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = Config {
		path: temp.path().join("test.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 8_000,
	};
	let cache = Cache::new(&config).unwrap();

	// Create object data and ID similar to server's write.rs.
	let content = b"hello world";
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(content),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

	// Put the object using sync function (like server does).
	let arg = tangram_cache::object::put::Arg {
		bytes: Some(bytes.clone()),
		checkout_pointer: None,
		id: id.clone(),
		length: Some(content.len().to_u64().unwrap()),
		put: [1; 16],
	};
	cache.put_object_sync(arg).unwrap();

	// Get the object using sync function.
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let result = cache.try_get_object_sync(&arg).unwrap().object;
	assert_eq!(
		result.and_then(|object| object.bytes),
		Some(Cow::Owned(bytes.to_vec()))
	);
}

// An object batch split across write transactions can be retrieved with the same bytes.
#[tokio::test]
async fn test_put_batch_and_get_object() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = Config {
		path: temp.path().join("test.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 1,
	};
	let cache = Cache::new(&config).unwrap();

	let content = b"hello world";
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(content),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
	let other_content = b"goodbye world";
	let other_data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(other_content),
	}));
	let other_bytes = other_data.serialize().unwrap();
	let other_id = tg::object::Id::new(tg::object::Kind::Blob, &other_bytes);

	cache
		.put_object_batch(vec![
			tangram_cache::object::put::Arg {
				bytes: Some(bytes.clone()),
				checkout_pointer: None,
				id: id.clone(),
				length: Some(content.len().to_u64().unwrap()),
				put: [1; 16],
			},
			tangram_cache::object::put::Arg {
				bytes: Some(other_bytes.clone()),
				checkout_pointer: None,
				id: other_id.clone(),
				length: Some(other_content.len().to_u64().unwrap()),
				put: [1; 16],
			},
		])
		.await
		.unwrap();

	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let result = cache.try_get_object(arg).await.unwrap().object;
	assert_eq!(
		result.and_then(|object| object.bytes),
		Some(Cow::Owned(bytes.to_vec()))
	);
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: other_id,
		put: None,
	};
	let result = cache.try_get_object(arg).await.unwrap().object;
	assert_eq!(
		result.and_then(|object| object.bytes),
		Some(Cow::Owned(other_bytes.to_vec()))
	);
}

// An object's length is persisted and replaced by later puts.
#[tokio::test]
async fn test_put_and_get_object_length() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = Config {
		path: temp.path().join("test.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 8_000,
	};
	let cache = Cache::new(&config).unwrap();

	let content = b"hello world";
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(content),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

	// Put an object with a length.
	let arg = tangram_cache::object::put::Arg {
		bytes: Some(bytes.clone()),
		checkout_pointer: None,
		id: id.clone(),
		length: Some(content.len().to_u64().unwrap()),
		put: [1; 16],
	};
	cache.put_object(arg).await.unwrap();
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
	assert_eq!(object.length, Some(content.len().to_u64().unwrap()));

	// A later put without a length replaces the length.
	let arg = tangram_cache::object::put::Arg {
		bytes: Some(bytes.clone()),
		checkout_pointer: None,
		id: id.clone(),
		length: None,
		put: [2; 16],
	};
	cache.put_object(arg).await.unwrap();
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
	assert_eq!(object.length, None);

	// An object put without a length has no length.
	let other = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"other"));
	let arg = tangram_cache::object::put::Arg {
		bytes: Some(bytes.clone()),
		checkout_pointer: None,
		id: other.clone(),
		length: None,
		put: [1; 16],
	};
	cache.put_object(arg).await.unwrap();
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: other,
		put: None,
	};
	let object = cache.try_get_object(arg).await.unwrap().object.unwrap();
	assert_eq!(object.length, None);

	// An absent object has no length.
	let absent = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"absent"));
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: absent,
		put: None,
	};
	let output = cache.try_get_object(arg).await.unwrap();
	assert!(output.object.is_none());
}

// Deleting an object removes the object.
#[tokio::test]
async fn test_delete_removes_object() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = Config {
		path: temp.path().join("test.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 8_000,
	};
	let cache = Cache::new(&config).unwrap();

	let content = b"hello world";
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(content),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

	let arg = tangram_cache::object::put::Arg {
		bytes: Some(bytes.clone()),
		checkout_pointer: None,
		id: id.clone(),
		length: Some(content.len().to_u64().unwrap()),
		put: [10; 16],
	};
	cache.put_object(arg).await.unwrap();
	let arg = tangram_cache::object::put::Arg {
		bytes: Some(Bytes::from_static(b"stale")),
		checkout_pointer: None,
		id: id.clone(),
		length: None,
		put: [9; 16],
	};
	cache.put_object(arg).await.unwrap();

	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let output = cache.try_get_object(arg).await.unwrap();
	let object = output.object.unwrap();
	assert_eq!(object.bytes, Some(Cow::Owned(bytes.to_vec())));
	assert_eq!(object.put, [10; 16]);

	let arg = tangram_cache::object::delete::Arg {
		id: id.clone(),
		put: [9; 16],
	};
	cache.delete_object(arg).await.unwrap();
	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	};
	let output = cache.try_get_object(arg).await.unwrap();
	assert!(output.object.is_some());

	let arg = tangram_cache::object::delete::Arg {
		id: id.clone(),
		put: [10; 16],
	};
	cache.delete_object(arg).await.unwrap();

	let arg = tangram_cache::object::get::Arg {
		bytes: true,
		id,
		put: None,
	};
	let output = cache.try_get_object(arg).await.unwrap();
	assert!(output.object.is_none());
}
