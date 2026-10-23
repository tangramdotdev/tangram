use {crate::Cache, tangram_cache::index, tangram_client::prelude::*};

impl Cache {
	pub fn delete_index_queue_fragment(&self, arg: index::queue::delete::Arg) {
		self.state()
			.index_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	#[must_use]
	pub fn get_index_queue_fragments(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> Vec<index::queue::Fragment> {
		self.state()
			.index_queue
			.range((arg.indexer.clone(), arg.sequence_start)..(arg.indexer, arg.sequence_end))
			.map(|(_, fragment)| fragment.clone())
			.collect()
	}

	pub fn put_index_queue_fragment(&self, arg: index::queue::put::Arg) {
		let fragment = arg.fragment;
		let key = (fragment.indexer.clone(), fragment.sequence);
		self.state().index_queue.insert(key, fragment);
	}

	#[must_use]
	pub fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> Option<index::queue::Fragment> {
		self.state()
			.index_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}
}

impl tangram_cache::index::Cache for Cache {
	async fn delete_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_fragment(arg);
		Ok(())
	}

	async fn get_index_queue_fragments(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Fragment>> {
		Ok(self.get_index_queue_fragments(arg))
	}

	async fn put_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_fragment(arg);
		Ok(())
	}

	async fn try_get_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Fragment>> {
		Ok(self.try_get_index_queue_fragment(arg))
	}
}
