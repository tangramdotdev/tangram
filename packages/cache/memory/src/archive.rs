use {crate::Cache, tangram_cache::archive, tangram_client::prelude::*};

impl Cache {
	pub fn delete_archive_queue_entry(&self, arg: archive::queue::delete::Arg) {
		self.state()
			.archive_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	#[must_use]
	pub fn get_archive_queue_entries(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> Vec<archive::queue::Entry> {
		self.state()
			.archive_queue
			.range((arg.indexer.clone(), arg.sequence_start)..(arg.indexer, arg.sequence_end))
			.map(|(_, entry)| entry.clone())
			.collect()
	}

	pub fn put_archive_queue_entry(&self, arg: archive::queue::put::Arg) {
		let entry = arg.entry;
		let key = (entry.indexer.clone(), entry.sequence);
		self.state().archive_queue.insert(key, entry);
	}

	#[must_use]
	pub fn try_get_archive_queue_entry(
		&self,
		arg: archive::queue::get::Arg,
	) -> Option<archive::queue::Entry> {
		self.state()
			.archive_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}
}

impl tangram_cache::archive::Cache for Cache {
	async fn delete_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_entry(arg);
		Ok(())
	}

	async fn get_archive_queue_entries(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Entry>> {
		Ok(self.get_archive_queue_entries(arg))
	}

	async fn put_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_entry(arg);
		Ok(())
	}

	async fn try_get_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Entry>> {
		Ok(self.try_get_archive_queue_entry(arg))
	}
}
