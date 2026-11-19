use {
	super::{ObjectPut, try_wait_for_object_puts},
	std::{
		sync::atomic::{AtomicUsize, Ordering},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_server_core::cache::object::{Object, get::Output},
};

#[tokio::test(start_paused = true)]
async fn an_archive_item_for_a_cached_put_does_not_wait() {
	let put = [1; 16];
	let (object, elapsed) = wait(put, put).await;
	assert!(matches!(&object, ObjectPut::Present(object) if object.put == put));
	assert_eq!(
		elapsed,
		Duration::ZERO,
		"an archive item whose put is in the cache should not wait"
	);
}

#[tokio::test(start_paused = true)]
async fn an_archive_item_for_a_replaced_put_does_not_wait() {
	let (object, elapsed) = wait([2; 16], [1; 16]).await;
	assert!(matches!(object, ObjectPut::Replaced));
	assert_eq!(
		elapsed,
		Duration::ZERO,
		"an archive item whose put was replaced by a newer put should not wait"
	);
}

#[tokio::test(start_paused = true)]
async fn an_archive_item_waits_for_its_put_while_the_cache_has_an_older_put() {
	// Put an object twice, where the cache has only the older put on the first read.
	let bytes = bytes::Bytes::from_static(b"hello");
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
	let older = [1; 16];
	let newer = [2; 16];
	let reads = AtomicUsize::new(0);
	let get = |ids: Vec<tg::object::Id>| {
		let put = if reads.fetch_add(1, Ordering::SeqCst) == 0 {
			older
		} else {
			newer
		};
		let outputs = ids.iter().map(|_| output(&bytes, put)).collect();
		async move { Ok(outputs) }
	};

	// Wait for the newer put, as the archive item of the second put does.
	let retry = tangram_server_core::config::ArchiveQueue::default().retry;
	let objects = try_wait_for_object_puts(&retry, &[(id, newer)], get)
		.await
		.unwrap();

	// The older put does not replace the newer put, so the archive item waits for the newer put and archives it.
	assert!(matches!(&objects[0], ObjectPut::Present(object) if object.put == newer));
	assert_eq!(reads.load(Ordering::SeqCst), 2);
}

/// Wait for an archive item's put while the cache has the cached put, and return the put's state and the time spent waiting.
async fn wait(cached: [u8; 16], put: [u8; 16]) -> (ObjectPut, Duration) {
	let bytes = bytes::Bytes::from_static(b"hello");
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
	let get = |ids: Vec<tg::object::Id>| {
		let outputs = ids.iter().map(|_| output(&bytes, cached)).collect();
		async move { Ok(outputs) }
	};
	let retry = tangram_server_core::config::ArchiveQueue::default().retry;
	let start = tokio::time::Instant::now();
	let mut objects = try_wait_for_object_puts(&retry, &[(id, put)], get)
		.await
		.unwrap();
	let elapsed = start.elapsed();

	(objects.remove(0), elapsed)
}

fn output(bytes: &bytes::Bytes, put: [u8; 16]) -> Output {
	let object = Object {
		bytes: Some(bytes.to_vec().into()),
		checkout_pointer: None,
		length: None,
		put,
	};
	Output {
		object: Some(object),
	}
}
