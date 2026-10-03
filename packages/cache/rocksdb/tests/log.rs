use {
	bytes::Bytes,
	std::collections::BTreeSet,
	tangram_cache::{log, log::read::Arg},
	tangram_client::{
		prelude::*,
		process::stdio::Stream::{Stderr, Stdout},
	},
};

#[tokio::test]
async fn log_end() {
	let (_temp, cache) = cache();
	completion(&cache).await;
}

#[tokio::test]
async fn log_read() {
	let (_temp, cache) = cache();
	read(&cache).await;
}

fn cache() -> (tangram_util::fs::Temp, tangram_cache_rocksdb::Cache) {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = tangram_cache_rocksdb::Config {
		path: temp.path().join("test.rocksdb"),
		read_batch_size: 64,
		read_concurrency: 1,
		write_batch_size: 64,
	};
	let cache = tangram_cache_rocksdb::Cache::new(&config).unwrap();
	(temp, cache)
}

async fn completion(cache: &impl tangram_cache::Cache) {
	for bytes in [Bytes::new(), Bytes::from_static(b"hello")] {
		let mut processes = std::array::from_fn::<_, 3, _>(|_| tg::process::Id::new());
		processes.sort();
		let [before, process, after] = processes;
		assert_eq!(cache.try_get_log_end(&process).await.unwrap(), None);
		let position = u64::try_from(bytes.len()).unwrap();
		let arg = log::put::Arg {
			bytes: bytes.clone(),
			position: 0,
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			stream_position: 0,
			timestamp: 0,
		};
		cache.put_log(arg).await.unwrap();
		let end = tg::process::log::End {
			position,
			stderr_position: 0,
			stdout_position: position,
		};
		let arg = log::end::Arg {
			end,
			process: process.clone(),
		};
		cache.put_log_end(arg.clone()).await.unwrap();
		cache.put_log_end(arg).await.unwrap();
		assert_eq!(cache.try_get_log_end(&process).await.unwrap(), Some(end));
		assert_eq!(cache.try_get_log_end(&before).await.unwrap(), None);
		assert_eq!(cache.try_get_log_end(&after).await.unwrap(), None);
		let arg = log::read::Arg {
			length: u64::MAX,
			position: 0,
			process: process.clone(),
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		};
		let entries = cache.try_read_log(arg).await.unwrap();
		let output = entries
			.into_iter()
			.flat_map(|entry| entry.bytes.into_owned())
			.collect::<Vec<_>>();
		assert_eq!(output, bytes);

		// Deleting a log must preserve the adjacent process's marker.
		let arg = log::end::Arg {
			end,
			process: after.clone(),
		};
		cache.put_log_end(arg).await.unwrap();
		let arg = log::delete::Arg {
			process: process.clone(),
		};
		cache.delete_log(arg).await.unwrap();
		assert_eq!(cache.try_get_log_end(&process).await.unwrap(), None);
		assert_eq!(cache.try_get_log_end(&after).await.unwrap(), Some(end));
	}
}

async fn read(cache: &impl tangram_cache::Cache) {
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
	assert_eq!(entries.len(), 3);
	assert_eq!((entries[0].position, entries[0].stream_position), (0, 0));
	assert_eq!((entries[1].position, entries[1].stream_position), (4, 2));
	assert_eq!((entries[2].position, entries[2].stream_position), (6, 4));
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

	// Keep the timestamps of adjacent writes to the same stream.
	let process = tg::process::Id::new();
	for (position, timestamp) in [(0, 1), (2, 2)] {
		let arg = log::put::Arg {
			bytes: Bytes::from_static(b"ab"),
			position,
			process: process.clone(),
			stream: Stdout,
			stream_position: position,
			timestamp,
		};
		cache.put_log(arg).await.unwrap();
	}
	let arg = Arg {
		length: u64::MAX,
		position: 0,
		process,
		streams: BTreeSet::from([Stdout]),
	};
	let entries = cache.try_read_log(arg).await.unwrap();
	assert_eq!(entries.len(), 2);
	assert_eq!((entries[0].timestamp, entries[1].timestamp), (1, 2));
}

async fn put(
	cache: &impl tangram_cache::Cache,
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

async fn bytes(cache: &impl tangram_cache::Cache, arg: &Arg) -> Vec<u8> {
	cache
		.try_read_log(arg.clone())
		.await
		.unwrap()
		.into_iter()
		.flat_map(|entry| entry.bytes.into_owned())
		.collect()
}
