use {super::*, bytes::Bytes, std::collections::BTreeSet};

mod cache;

fn collect_bytes(entries: Vec<log::read::Entry<'_>>) -> Bytes {
	entries
		.into_iter()
		.flat_map(|entry| entry.bytes.to_vec())
		.collect::<Vec<_>>()
		.into()
}

#[test]
fn put_retry_is_idempotent() {
	let cache = Cache::new();
	let process = tg::process::Id::new();
	let arg = log::put::Arg {
		bytes: Bytes::from_static(b"hello"),
		position: 0,
		process: process.clone(),
		stream: tg::process::stdio::Stream::Stdout,
		stream_position: 0,
		timestamp: 1,
	};
	cache.put_log(arg.clone());
	cache.put_log(arg);
	let entries = cache.try_read_log(log::read::Arg {
		length: u64::MAX,
		position: 0,
		process: process.clone(),
		streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
	});
	assert_eq!(collect_bytes(entries), Bytes::from_static(b"hello"));
	assert_eq!(
		cache.try_get_log_length(&log::length::Arg {
			process,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		}),
		Some(5)
	);
}
