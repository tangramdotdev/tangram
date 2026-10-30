mod ancestor;
mod batch;
mod capture;
mod clean;
mod delegation;
mod indexer;
mod location;
mod object;
mod page;
mod process;
mod reader;
mod storage;
mod sync;
mod tag;
mod update;
mod usage;
mod verify;
mod versions;

use super::{Config, Index};

/// The usage delta time to live that the tests aggregate and read usage with.
const USAGE_DELTA_TIME_TO_LIVE: std::time::Duration = std::time::Duration::from_hours(1);

fn new_index() -> (tempfile::TempDir, Index) {
	new_index_with_usage_partition_total(1)
}

fn new_index_with_usage_partition_total(usage_partition_total: u64) -> (tempfile::TempDir, Index) {
	new_index_with_options(usage_partition_total, std::time::Duration::ZERO)
}

fn new_index_with_permission_time_to_touch(
	time_to_touch: std::time::Duration,
) -> (tempfile::TempDir, Index) {
	new_index_with_options(1, time_to_touch)
}

fn new_index_with_options(
	usage_partition_total: u64,
	permission_time_to_touch: std::time::Duration,
) -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let config = Config {
		map_size: 1 << 30,
		max_process_depth: None,
		max_readers: 1_000,
		object_permission_time_to_touch: permission_time_to_touch,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: permission_time_to_touch,
		read_channel_capacity: 256,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total,
		write_channel_capacity: 256,
		write_operation_batch_size: 1,
	};
	let index = Index::new(&config).unwrap();
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
