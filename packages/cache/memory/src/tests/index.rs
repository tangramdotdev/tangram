use {crate::Cache, bytes::Bytes, tangram_cache::index, tangram_client::prelude::*};

#[test]
fn index() {
	let cache = Cache::new();
	let indexer = tg::indexer::Id::new();
	let fragment = index::queue::Fragment {
		batch: index::queue::batch::Id::new([1; 16]),
		fragment: 1,
		fragments: 2,
		indexer: indexer.clone(),
		payload: Bytes::from_static(b"payload"),
		sequence: 42,
	};
	let arg = index::queue::put::Arg {
		fragment: fragment.clone(),
	};
	cache.put_index_queue_fragment(arg);
	let arg = index::queue::get::Arg {
		indexer: indexer.clone(),
		sequence: 42,
	};
	assert_eq!(
		cache.try_get_index_queue_fragment(arg),
		Some(fragment.clone())
	);
	let arg = index::queue::get::batch::Arg {
		indexer: indexer.clone(),
		sequence_end: 43,
		sequence_start: 42,
	};
	assert_eq!(cache.get_index_queue_fragments(arg), vec![fragment]);
	let arg = index::queue::delete::Arg {
		indexer: indexer.clone(),
		sequence: 42,
	};
	cache.delete_index_queue_fragment(arg);
	let arg = index::queue::get::Arg {
		indexer,
		sequence: 42,
	};
	assert!(cache.try_get_index_queue_fragment(arg).is_none());
}
