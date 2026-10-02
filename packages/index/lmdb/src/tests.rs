mod ancestor;
mod batch;
mod capture;
mod clean;
mod delegation;
mod indexer;
mod location;
mod log;
mod object;
mod reader;
mod storage;
mod tag;
mod update;
mod usage;
mod verify;
mod versions;

use super::{Config, Index};

fn new_index() -> (tempfile::TempDir, Index) {
	new_index_with_usage_partition_total(1)
}

fn new_index_with_usage_partition_total(usage_partition_total: u64) -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total,
		write_operation_batch_size: 1,
	})
	.unwrap();
	let mut transaction = index.env.write_txn().unwrap();
	let key = Index::pack(
		&index.subspace,
		&crate::Key::Usage(crate::usage::Key::Started),
	);
	let value = tangram_index::usage::serialize_timestamp(i64::MIN);
	index.db.put(&mut transaction, &key, &value).unwrap();
	transaction.commit().unwrap();

	(dir, index)
}
