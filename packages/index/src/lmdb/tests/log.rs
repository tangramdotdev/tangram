use tangram_client::prelude::*;

#[tokio::test]
async fn deduplicates_and_completes_log_compactions() {
	let (_dir, index) = super::new_index();
	let process = tg::process::Id::new();

	index.enqueue_log_compaction(&process).await.unwrap();
	index.enqueue_log_compaction(&process).await.unwrap();

	let entries = index.log_compaction_batch(10, 0, 1).await.unwrap();
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].process, process);
	assert!(
		index
			.try_get_oldest_log_compaction_transaction_id()
			.await
			.unwrap()
			.is_some()
	);

	index.complete_log_compaction(&entries[0]).await.unwrap();

	assert!(
		index
			.log_compaction_batch(10, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
	assert_eq!(
		index
			.try_get_oldest_log_compaction_transaction_id()
			.await
			.unwrap(),
		None
	);
}
