use tangram_client::prelude::*;

#[tokio::test]
async fn finished_syncs_are_recorded_until_they_expire() {
	let (_dir, index) = super::new_index();
	let expired = tg::sync::Id::new();
	let current = tg::sync::Id::new();
	let missing = tg::sync::Id::new();

	// Record two finished syncs, and extend the expiration of one of them.
	let items = [(&expired, 10), (&current, 10), (&current, 2000)]
		.into_iter()
		.map(|(id, expires_at)| {
			let arg = tangram_index::sync::put::Arg {
				expires_at,
				id: id.clone(),
			};
			tangram_index::Item::PutSync(arg)
		})
		.collect();
	let arg = tangram_index::write::Arg { items };
	index.write(arg).await.unwrap().unwrap();
	let ids = [expired.clone(), current.clone(), missing.clone()];
	let syncs = index.try_get_syncs(&ids).await.unwrap();
	assert_eq!(syncs[0].as_ref().map(|sync| sync.expires_at), Some(10));
	assert_eq!(syncs[1].as_ref().map(|sync| sync.expires_at), Some(2000));
	assert!(syncs[2].is_none());

	// Clean the expired sync and keep the other.
	let arg = tangram_index::clean::Arg {
		batch_size: 100,
		max_object_touched_at: i64::MIN,
		max_process_touched_at: i64::MIN,
		max_sandbox_touched_at: i64::MIN,
		now: 1000,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();
	let syncs = index.try_get_syncs(&ids).await.unwrap();
	assert!(syncs[0].is_none());
	assert_eq!(syncs[1].as_ref().map(|sync| sync.expires_at), Some(2000));
	assert!(syncs[2].is_none());
}
