mod ancestor;
mod authorize;
mod batch;
mod clean;
mod log;
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
			object_subtree: crate::authorize::ObjectSubtreeConfig::default(),
			process_subtree: crate::authorize::ProcessSubtreeConfig::default(),
		},
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		read_batch_size: 64,
		read_concurrency: 4,
		usage_partition_total,
		write_batch_size: 1,
	})
	.unwrap();

	(dir, index)
}
