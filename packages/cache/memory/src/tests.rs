use {
	super::*,
	bytes::Bytes,
	num::ToPrimitive as _,
	std::{borrow::Cow, path::PathBuf},
};

mod archive;
mod index;
mod object;

// A put replaces an existing object.
#[test]
fn put_replaces_object() {
	let cache = Cache::default();
	let first_bytes = Bytes::from_static(b"first");
	let id = tg::object::Id::new(tg::object::Kind::Blob, &first_bytes);
	let checkout_pointer = cache_object::checkout::Pointer {
		artifact: tg::file::Id::new(b"first").into(),
		length: 5,
		path: Some(PathBuf::from("first")),
		position: 1,
	};
	cache
		.put_object(cache_object::put::Arg {
			bytes: Some(first_bytes),
			checkout_pointer: Some(checkout_pointer),
			id: id.clone(),
			length: Some(5),
			put: [1; 16],
		})
		.unwrap();

	let second_bytes = Bytes::from_static(b"second");
	cache
		.put_object_batch(vec![cache_object::put::Arg {
			bytes: Some(second_bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: None,
			put: [2; 16],
		}])
		.unwrap();

	let object = cache
		.try_get_object_sync(&cache_object::get::Arg {
			bytes: true,
			id,
			put: None,
		})
		.object
		.unwrap();
	assert_eq!(object.bytes, Some(Cow::Owned(second_bytes.to_vec())));
	assert!(object.checkout_pointer.is_none());
	assert!(object.length.is_none());
	assert_eq!(object.put, [2; 16]);
}

// An exact get returns only the requested put.
#[tokio::test]
async fn get_exact_put() {
	let cache = Cache::default();
	let bytes = Bytes::from_static(b"bytes");
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
	cache
		.put_object(cache_object::put::Arg {
			bytes: Some(bytes),
			checkout_pointer: None,
			id: id.clone(),
			length: None,
			put: [2; 16],
		})
		.unwrap();

	let output = cache.try_get_object_sync(&cache_object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: Some([1; 16]),
	});
	assert!(output.object.is_none());
	let contains = tangram_cache::object::Cache::contains_object(
		&cache,
		cache_object::contains::Arg {
			id: id.clone(),
			put: [1; 16],
		},
	)
	.await
	.unwrap();
	assert!(!contains);
	let output = cache.try_get_object_sync(&cache_object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: Some([2; 16]),
	});
	assert_eq!(output.object.unwrap().put, [2; 16]);
	let contains = tangram_cache::object::Cache::contains_object(
		&cache,
		cache_object::contains::Arg { id, put: [2; 16] },
	)
	.await
	.unwrap();
	assert!(contains);
}

// Deleting an object removes the object.
#[test]
fn delete_removes_object() {
	let cache = Cache::default();
	let content = b"hello world";
	let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
		bytes: Bytes::from_static(content),
	}));
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

	cache
		.put_object(cache_object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: Some(content.len().to_u64().unwrap()),
			put: [10; 16],
		})
		.unwrap();
	cache
		.put_object(cache_object::put::Arg {
			bytes: Some(Bytes::from_static(b"stale")),
			checkout_pointer: None,
			id: id.clone(),
			length: None,
			put: [9; 16],
		})
		.unwrap();

	let output = cache.try_get_object_sync(&cache_object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	});
	let object = output.object.unwrap();
	assert_eq!(object.bytes, Some(Cow::Owned(bytes.to_vec())));
	assert_eq!(object.put, [10; 16]);

	cache
		.delete_object(cache_object::delete::Arg {
			id: id.clone(),
			put: [9; 16],
		})
		.unwrap();
	let output = cache.try_get_object_sync(&cache_object::get::Arg {
		bytes: true,
		id: id.clone(),
		put: None,
	});
	assert!(output.object.is_some());

	cache
		.delete_object(cache_object::delete::Arg {
			id: id.clone(),
			put: [10; 16],
		})
		.unwrap();

	let output = cache.try_get_object_sync(&cache_object::get::Arg {
		bytes: true,
		id,
		put: None,
	});
	assert!(output.object.is_none());
}
