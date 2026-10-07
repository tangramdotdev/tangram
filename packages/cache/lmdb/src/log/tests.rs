use {
	super::*,
	bytes::Bytes,
	std::{collections::BTreeSet, path::Path},
};

mod cache;

fn cache(path: &Path) -> Cache {
	let config = super::super::Config {
		map_size: 10 * 1024 * 1024,
		path: path.join("test.lmdb"),
		posix_sem_prefix: None,
		read_batch_size: 64,
		read_concurrency: 4,
		write_batch_size: 8_000,
	};
	Cache::new(&config).unwrap()
}

fn streams(
	streams: impl IntoIterator<Item = tg::process::stdio::Stream>,
) -> BTreeSet<tg::process::stdio::Stream> {
	streams.into_iter().collect()
}

async fn put(
	cache: &Cache,
	process: &tg::process::Id,
	bytes: &'static [u8],
	position: u64,
	stream: tg::process::stdio::Stream,
	stream_position: u64,
) {
	let arg = log::put::Arg {
		bytes: Bytes::from_static(bytes),
		position,
		process: process.clone(),
		stream,
		stream_position,
		timestamp: i64::try_from(position).unwrap(),
	};
	cache.put_log(arg).await.unwrap();
}

fn bytes(entries: &[log::read::Entry<'_>]) -> Bytes {
	entries
		.iter()
		.flat_map(|entry| entry.bytes.iter().copied())
		.collect::<Vec<_>>()
		.into()
}

#[tokio::test]
async fn read_and_length() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let cache = cache(temp.path());
	let process = tg::process::Id::new();
	put(
		&cache,
		&process,
		b"abc",
		0,
		tg::process::stdio::Stream::Stdout,
		0,
	)
	.await;
	put(
		&cache,
		&process,
		b"de",
		3,
		tg::process::stdio::Stream::Stderr,
		0,
	)
	.await;
	put(
		&cache,
		&process,
		b"fghi",
		5,
		tg::process::stdio::Stream::Stdout,
		3,
	)
	.await;

	let combined_streams = streams([
		tg::process::stdio::Stream::Stderr,
		tg::process::stdio::Stream::Stdout,
	]);
	let entries = cache
		.try_read_log(log::read::Arg {
			length: 6,
			position: 1,
			process: process.clone(),
			streams: combined_streams.clone(),
		})
		.await
		.unwrap();
	assert_eq!(bytes(&entries), Bytes::from_static(b"bcdefg"));

	let stdout_streams = streams([tg::process::stdio::Stream::Stdout]);
	let entries = cache
		.try_read_log(log::read::Arg {
			length: 4,
			position: 2,
			process: process.clone(),
			streams: stdout_streams.clone(),
		})
		.await
		.unwrap();
	assert_eq!(bytes(&entries), Bytes::from_static(b"cfgh"));

	let length = cache
		.try_get_log_length(log::length::Arg {
			process: process.clone(),
			streams: combined_streams,
		})
		.await
		.unwrap();
	assert_eq!(length, Some(9));
	let length = cache
		.try_get_log_length(log::length::Arg {
			process,
			streams: stdout_streams,
		})
		.await
		.unwrap();
	assert_eq!(length, Some(7));
}

#[tokio::test]
async fn retry_and_delete() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let cache = cache(temp.path());
	let process = tg::process::Id::new();
	for _ in 0..2 {
		put(
			&cache,
			&process,
			b"hello",
			0,
			tg::process::stdio::Stream::Stdout,
			0,
		)
		.await;
	}
	let streams = streams([tg::process::stdio::Stream::Stdout]);
	let entries = cache
		.try_read_log(log::read::Arg {
			length: u64::MAX,
			position: 0,
			process: process.clone(),
			streams: streams.clone(),
		})
		.await
		.unwrap();
	assert_eq!(bytes(&entries), Bytes::from_static(b"hello"));

	cache
		.delete_log(log::delete::Arg {
			process: process.clone(),
		})
		.await
		.unwrap();
	let entries = cache
		.try_read_log(log::read::Arg {
			length: u64::MAX,
			position: 0,
			process: process.clone(),
			streams: streams.clone(),
		})
		.await
		.unwrap();
	assert!(entries.is_empty());
	let length = cache
		.try_get_log_length(log::length::Arg { process, streams })
		.await
		.unwrap();
	assert_eq!(length, None);
}
