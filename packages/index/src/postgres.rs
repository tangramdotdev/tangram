mod clean;
mod indexer;
mod metadata;
mod stored;
mod touch;
mod transaction;

use {
	crate::{
		CleanOutput, DeleteTagArg, ObjectStored, ProcessStored, PutCacheEntryArg, PutObjectArg,
		PutProcessArg, PutTagArg, TouchObjectArg, TouchProcessArg,
	},
	tangram_client::prelude::*,
	tangram_database as db,
};

pub struct Index {
	database: db::postgres::Database,
}

impl Index {
	#[must_use]
	pub fn new(database: db::postgres::Database) -> Self {
		Self { database }
	}
}

impl crate::Index for Index {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.try_get_object_metadata(id).await
	}

	async fn try_get_object_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata_batch(ids).await
	}

	async fn try_get_object_stored(&self, id: &tg::object::Id) -> tg::Result<Option<ObjectStored>> {
		self.try_get_object_stored(id).await
	}

	async fn try_get_object_stored_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<ObjectStored>>> {
		self.try_get_object_stored_batch(ids).await
	}

	async fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		self.try_get_object_stored_and_metadata(id).await
	}

	async fn try_get_object_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		self.try_get_object_stored_and_metadata_batch(ids).await
	}

	async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		self.try_touch_object_and_get_stored_and_metadata(id, touched_at)
			.await
	}

	async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		self.try_touch_object_and_get_stored_and_metadata_batch(ids, touched_at)
			.await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.try_get_process_metadata(id).await
	}

	async fn try_get_process_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		self.try_get_process_metadata_batch(ids).await
	}

	async fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<ProcessStored>> {
		self.try_get_process_stored(id).await
	}

	async fn try_get_process_stored_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<ProcessStored>>> {
		self.try_get_process_stored_batch(ids).await
	}

	async fn try_get_process_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		self.try_get_process_stored_and_metadata_batch(ids).await
	}

	async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
		self.try_touch_process_and_get_stored_and_metadata(id, touched_at)
			.await
	}

	async fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		self.try_touch_process_and_get_stored_and_metadata_batch(ids, touched_at)
			.await
	}

	async fn handle_messages(
		&self,
		put_cache_entry_messages: Vec<PutCacheEntryArg>,
		put_object_messages: Vec<PutObjectArg>,
		touch_object_messages: Vec<TouchObjectArg>,
		put_process_messages: Vec<PutProcessArg>,
		touch_process_messages: Vec<TouchProcessArg>,
		put_tag_messages: Vec<PutTagArg>,
		delete_tag_messages: Vec<DeleteTagArg>,
	) -> tg::Result<()> {
		self.handle_messages(
			put_cache_entry_messages,
			put_object_messages,
			touch_object_messages,
			put_process_messages,
			touch_process_messages,
			put_tag_messages,
			delete_tag_messages,
		)
		.await
	}

	async fn handle_queue(&self, batch_size: usize) -> tg::Result<usize> {
		self.handle_queue(batch_size).await
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn get_queue_size(&self, transaction_id: u64) -> tg::Result<u64> {
		self.get_queue_size(transaction_id).await
	}

	async fn clean(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		self.clean(max_touched_at, n).await
	}

	async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		self.touch_object(id).await
	}

	async fn touch_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		self.touch_process(id).await
	}

	async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}
}
