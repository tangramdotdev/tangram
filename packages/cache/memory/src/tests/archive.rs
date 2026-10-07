use {crate::Cache, bytes::Bytes, tangram_cache::archive, tangram_client::prelude::*};

#[test]
fn archive() {
	let cache = Cache::new();
	let indexer = tg::indexer::Id::new();
	let object = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
	let entry = archive::queue::Entry {
		indexer: indexer.clone(),
		object,
		put: [1; 16],
		sequence: 42,
	};
	let arg = archive::queue::put::Arg {
		entry: entry.clone(),
	};
	cache.put_archive_queue_entry(arg);
	let arg = archive::queue::get::Arg {
		indexer: indexer.clone(),
		sequence: 42,
	};
	assert_eq!(cache.try_get_archive_queue_entry(arg), Some(entry.clone()));
	let arg = archive::queue::get::batch::Arg {
		indexer: indexer.clone(),
		sequence_end: 43,
		sequence_start: 42,
	};
	assert_eq!(cache.get_archive_queue_entries(arg), vec![entry]);
	let arg = archive::queue::delete::Arg {
		indexer: indexer.clone(),
		sequence: 42,
	};
	cache.delete_archive_queue_entry(arg);
	let arg = archive::queue::get::Arg {
		indexer,
		sequence: 42,
	};
	assert!(cache.try_get_archive_queue_entry(arg).is_none());
}
