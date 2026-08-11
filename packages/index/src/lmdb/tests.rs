mod ancestor;
mod authorize;
mod batch;
mod reader;
mod storage;

use super::{Config, Index};

fn new_index() -> (tempfile::TempDir, Index) {
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
		storage_partition_total: 1,
		write_batch_size: 1,
	})
	.unwrap();

	(dir, index)
}
