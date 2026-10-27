use {crate::Cache, bytes::Bytes, tangram_cache::index, tangram_client::prelude::*};

#[test]
fn index() {
	let cache = Cache::new();
	let indexer = tg::indexer::Id::new();
	let item = index::queue::Item {
		batch: index::queue::batch::Id::new([1; 16]),
		fragment: 1,
		fragments: 2,
		indexer: indexer.clone(),
		payload: Bytes::from_static(b"payload"),
		sequence: 42,
	};
	let arg = index::queue::put::Arg { item: item.clone() };
	cache.put_index_queue_item(arg);
	let arg = index::queue::get::Arg {
		indexer: indexer.clone(),
		sequence: 42,
	};
	assert_eq!(cache.try_get_index_queue_item(arg), Some(item.clone()));
	let arg = index::queue::get::batch::Arg {
		indexer: indexer.clone(),
		sequence_end: 43,
		sequence_start: 42,
	};
	assert_eq!(cache.get_index_queue_items(arg), vec![item]);
	let arg = index::queue::delete::batch::Arg {
		indexer: indexer.clone(),
		sequences: vec![42],
	};
	cache.delete_index_queue_item_batch(arg);
	let arg = index::queue::get::Arg {
		indexer,
		sequence: 42,
	};
	assert!(cache.try_get_index_queue_item(arg).is_none());
}
