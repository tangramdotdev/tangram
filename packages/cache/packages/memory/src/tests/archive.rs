use {crate::Cache, bytes::Bytes, tangram_cache::archive, tangram_client::prelude::*};

#[test]
fn archive() {
	let cache = Cache::new();
	let indexer = tg::indexer::Id::new();
	let item = archive::queue::Item {
		batch: archive::queue::batch::Id::new([1; 16]),
		fragment: 1,
		fragments: 2,
		indexer: indexer.clone(),
		payload: Bytes::from_static(b"payload"),
		sequence: 42,
	};
	let arg = archive::queue::put::Arg { item: item.clone() };
	cache.put_archive_queue_item(arg);
	let arg = archive::queue::get::Arg {
		indexer: indexer.clone(),
		sequence: 42,
	};
	assert_eq!(cache.try_get_archive_queue_item(arg), Some(item.clone()));
	let arg = archive::queue::get::batch::Arg {
		indexer: indexer.clone(),
		sequence_end: 43,
		sequence_start: 42,
	};
	assert_eq!(cache.get_archive_queue_items(arg), vec![item]);
	let arg = archive::queue::delete::batch::Arg {
		indexer: indexer.clone(),
		sequences: vec![42],
	};
	cache.delete_archive_queue_item_batch(arg);
	let arg = archive::queue::get::Arg {
		indexer,
		sequence: 42,
	};
	assert!(cache.try_get_archive_queue_item(arg).is_none());
}
