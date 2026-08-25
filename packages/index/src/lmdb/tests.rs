mod ancestor;
mod authorize;
mod batch;
mod clean;
mod log;
mod process_object_grant;
mod reader;
mod storage;
mod update;
mod usage;

use super::{Config, Index};

fn new_index() -> (tempfile::TempDir, Index) {
	new_index_with_usage_partition_total(1)
}

fn new_index_with_usage_partition_total(usage_partition_total: u64) -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		authorize: super::AuthorizeConfig {
			ancestor: crate::authorize::SearchConfig::default(),
			descendant: crate::authorize::SearchConfig::default(),
			subtree: crate::authorize::SubtreeConfig::default(),
		},
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total,
		write_operation_batch_size: 1,
	})
	.unwrap();
	let mut transaction = index.env.write_txn().unwrap();
	let key = Index::pack(
		&index.subspace,
		&crate::lmdb::Key::Usage(crate::lmdb::usage::Key::Started),
	);
	let value = crate::usage::serialize_timestamp(i64::MIN);
	index.db.put(&mut transaction, &key, &value).unwrap();
	transaction.commit().unwrap();

	(dir, index)
}
