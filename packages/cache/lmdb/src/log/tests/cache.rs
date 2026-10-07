use {
	crate::Cache, bytes::Bytes, std::collections::BTreeSet, tangram_cache::log,
	tangram_client::prelude::*,
};

#[tokio::test]
async fn expiration_deletes_the_complete_log_and_preserves_other_entries() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let config = crate::Config {
		map_size: 10 * 1024 * 1024,
		posix_sem_prefix: None,
		path: temp.path().join("cache"),
		read_batch_size: 64,
		read_concurrency: 1,
		write_batch_size: 64,
	};
	let cache = Cache::new(&config).unwrap();
	let process = tg::process::Id::new();
	let other = tg::process::Id::new();
	let stream = tg::process::stdio::Stream::Stdout;
	let arg = log::put::Arg {
		bytes: Bytes::from_static(b"log"),
		position: 0,
		process: process.clone(),
		stream,
		stream_position: 0,
		timestamp: 0,
	};
	cache.put_log(arg).await.unwrap();
	let arg = log::end::Arg {
		end: tg::process::log::End {
			position: 3,
			stderr_position: 0,
			stdout_position: 3,
		},
		process: process.clone(),
	};
	cache.put_log_end(arg).await.unwrap();

	for (expires_at, partition, process) in [
		(10, 0, process.clone()),
		(20, 0, other.clone()),
		(5, 1, other.clone()),
	] {
		let entry = log::cache::Entry {
			expires_at,
			partition,
			process,
		};
		let arg = log::cache::put::Arg { entry };
		cache.put_log_cache_entry(arg.clone()).await.unwrap();
		cache.put_log_cache_entry(arg).await.unwrap();
	}

	let arg = log::cache::get::Arg {
		batch_size: 10,
		now: 9,
		partition: 0,
	};
	assert_eq!(cache.get_log_cache_entries(arg).await.unwrap(), []);
	let arg = log::cache::get::Arg { now: 10, ..arg };
	let entries = cache.get_log_cache_entries(arg).await.unwrap();
	assert_eq!(entries.len(), 1);

	let arg = log::cache::delete::Arg {
		entry: entries[0].clone(),
	};
	cache.delete_log_cache_entry(arg).await.unwrap();
	assert!(cache.try_get_log_end(&process).await.unwrap().is_none());

	let arg = log::read::Arg {
		length: 10,
		position: 0,
		process,
		streams: BTreeSet::from([stream]),
	};
	assert!(cache.try_read_log(arg).await.unwrap().is_empty());

	let arg = log::cache::get::Arg {
		batch_size: 1,
		now: 20,
		partition: 0,
	};
	assert_eq!(
		cache.get_log_cache_entries(arg).await.unwrap()[0].process,
		other
	);
	let arg = log::cache::get::Arg {
		partition: 1,
		..arg
	};
	assert_eq!(cache.get_log_cache_entries(arg).await.unwrap().len(), 1);
}
