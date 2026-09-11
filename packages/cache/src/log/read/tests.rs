use {
	super::*,
	crate::log,
	bytes::Bytes,
	tg::process::stdio::Stream::{Stderr, Stdout},
};

#[tokio::test]
async fn memory() {
	read(&crate::memory::Cache::new()).await;
}

#[cfg(feature = "lmdb")]
#[tokio::test]
async fn lmdb() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = crate::lmdb::Config {
		map_size: 10 * 1024 * 1024,
		path: temp.path().join("test.lmdb"),
		posix_sem_prefix: None,
		read_batch_size: 64,
		read_concurrency: 1,
		write_batch_size: 64,
	};
	let cache = crate::lmdb::Cache::new(&config).unwrap();
	read(&cache).await;
}

async fn read(cache: &impl crate::Cache) {
	let process = tg::process::Id::new();
	let mut arg = Arg {
		length: u64::MAX,
		position: 0,
		process: process.clone(),
		streams: BTreeSet::from([Stderr, Stdout]),
	};

	// A later chunk must not hide a missing initial chunk.
	put(cache, &process, 6, Stdout, 4, b"gh").await;
	assert!(cache.try_read_log(arg.clone()).await.unwrap().is_empty());
	arg.streams = BTreeSet::from([Stdout]);
	assert!(cache.try_read_log(arg.clone()).await.unwrap().is_empty());

	// Neither a combined read nor a single stream read can cross a gap in that stream.
	put(cache, &process, 0, Stdout, 0, b"ab").await;
	for streams in [BTreeSet::from([Stderr, Stdout]), BTreeSet::from([Stdout])] {
		arg.streams = streams;
		assert_eq!(bytes(cache, &arg).await, b"ab");
		arg.position = 2;
		assert!(cache.try_read_log(arg.clone()).await.unwrap().is_empty());
		arg.position = 0;
	}

	// Filling the stdout gap lets that stream proceed even while stderr is still missing.
	put(cache, &process, 4, Stdout, 2, b"ef").await;
	assert_eq!(bytes(cache, &arg).await, b"abefgh");
	let entries = cache.try_read_log(arg.clone()).await.unwrap();
	assert_eq!(entries.len(), 2);
	assert_eq!((entries[0].position, entries[0].stream_position), (0, 0));
	assert_eq!((entries[1].position, entries[1].stream_position), (4, 2));
	arg.streams = BTreeSet::from([Stderr, Stdout]);
	assert_eq!(bytes(cache, &arg).await, b"ab");

	// Filling the combined gap exposes every byte exactly once.
	put(cache, &process, 2, Stderr, 0, b"cd").await;
	assert_eq!(bytes(cache, &arg).await, b"abcdefgh");
	arg.position = 1;
	arg.length = 6;
	assert_eq!(bytes(cache, &arg).await, b"bcdefg");
	arg.position = 7;
	assert_eq!(bytes(cache, &arg).await, b"h");
	arg.position = 8;
	assert!(cache.try_read_log(arg.clone()).await.unwrap().is_empty());
	arg.position = 0;
	arg.length = 0;
	assert!(cache.try_read_log(arg).await.unwrap().is_empty());
}

async fn put(
	cache: &impl crate::Cache,
	process: &tg::process::Id,
	position: u64,
	stream: tg::process::stdio::Stream,
	stream_position: u64,
	bytes: &'static [u8],
) {
	let arg = log::put::Arg {
		bytes: Bytes::from_static(bytes),
		position,
		process: process.clone(),
		stream,
		stream_position,
		timestamp: 0,
	};
	cache.put_log(arg).await.unwrap();
}

async fn bytes(cache: &impl crate::Cache, arg: &Arg) -> Vec<u8> {
	cache
		.try_read_log(arg.clone())
		.await
		.unwrap()
		.into_iter()
		.flat_map(|entry| entry.bytes.into_owned())
		.collect()
}
