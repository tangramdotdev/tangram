use {crate::Cache, tangram_cache::archive, tangram_client::prelude::*};

impl Cache {
	pub fn delete_archive_queue_item(&self, arg: archive::queue::delete::Arg) {
		self.state()
			.archive_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	pub fn delete_archive_queue_item_batch(&self, arg: archive::queue::delete::batch::Arg) {
		let mut state = self.state();
		for sequence in arg.sequences {
			state.archive_queue.remove(&(arg.indexer.clone(), sequence));
		}
	}

	#[must_use]
	pub fn get_archive_queue_items(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> Vec<archive::queue::Item> {
		self.state()
			.archive_queue
			.range((arg.indexer.clone(), arg.sequence_start)..(arg.indexer, arg.sequence_end))
			.map(|(_, item)| item.clone())
			.collect()
	}

	pub fn put_archive_queue_item(&self, arg: archive::queue::put::Arg) {
		let item = arg.item;
		let key = (item.indexer.clone(), item.sequence);
		self.state().archive_queue.insert(key, item);
	}

	#[must_use]
	pub fn try_get_archive_queue_item(
		&self,
		arg: archive::queue::get::Arg,
	) -> Option<archive::queue::Item> {
		self.state()
			.archive_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}
}

impl tangram_cache::archive::Cache for Cache {
	async fn delete_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_item(arg);
		Ok(())
	}

	async fn delete_archive_queue_item_batch(
		&self,
		arg: tangram_cache::archive::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_item_batch(arg);
		Ok(())
	}

	async fn get_archive_queue_items(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Item>> {
		Ok(self.get_archive_queue_items(arg))
	}

	async fn put_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_item(arg);
		Ok(())
	}

	async fn try_get_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Item>> {
		Ok(self.try_get_archive_queue_item(arg))
	}
}
