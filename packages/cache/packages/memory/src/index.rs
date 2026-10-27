use {crate::Cache, tangram_cache::index, tangram_client::prelude::*};

impl Cache {
	pub fn delete_index_queue_item(&self, arg: index::queue::delete::Arg) {
		self.state()
			.index_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	pub fn delete_index_queue_item_batch(&self, arg: index::queue::delete::batch::Arg) {
		let mut state = self.state();
		for sequence in arg.sequences {
			state.index_queue.remove(&(arg.indexer.clone(), sequence));
		}
	}

	#[must_use]
	pub fn get_index_queue_items(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> Vec<index::queue::Item> {
		self.state()
			.index_queue
			.range((arg.indexer.clone(), arg.sequence_start)..(arg.indexer, arg.sequence_end))
			.map(|(_, item)| item.clone())
			.collect()
	}

	pub fn put_index_queue_item(&self, arg: index::queue::put::Arg) {
		let item = arg.item;
		let key = (item.indexer.clone(), item.sequence);
		self.state().index_queue.insert(key, item);
	}

	#[must_use]
	pub fn try_get_index_queue_item(
		&self,
		arg: index::queue::get::Arg,
	) -> Option<index::queue::Item> {
		self.state()
			.index_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}
}

impl tangram_cache::index::Cache for Cache {
	async fn delete_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_item(arg);
		Ok(())
	}

	async fn delete_index_queue_item_batch(
		&self,
		arg: tangram_cache::index::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_item_batch(arg);
		Ok(())
	}

	async fn get_index_queue_items(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Item>> {
		Ok(self.get_index_queue_items(arg))
	}

	async fn put_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_item(arg);
		Ok(())
	}

	async fn try_get_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Item>> {
		Ok(self.try_get_index_queue_item(arg))
	}
}
