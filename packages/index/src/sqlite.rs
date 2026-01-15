use {
	crate::{
		CleanOutput, DeleteTagArg, ObjectStored, ProcessStored, PutCacheEntryArg, PutObjectArg,
		PutProcessArg, PutTagArg, TouchObjectArg, TouchProcessArg,
	},
	futures::FutureExt as _,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

mod clean;
mod indexer;
mod metadata;
mod queue;
mod stored;
mod touch;
mod transaction;

static SQL: &str = include_str!("sqlite.sql");

pub struct Index {
	database: db::sqlite::Database,
}

impl Index {
	#[must_use]
	pub fn new(database: db::sqlite::Database) -> Self {
		Self { database }
	}

	pub async fn migrate(&self) -> tg::Result<()> {
		migrate(&self.database).await
	}
}

pub fn initialize(connection: &sqlite::Connection) -> sqlite::Result<()> {
	connection.pragma_update(None, "auto_vaccum", "incremental")?;
	connection.pragma_update(None, "busy_timeout", "5000")?;
	connection.pragma_update(None, "cache_size", "-20000")?;
	connection.pragma_update(None, "foreign_keys", "on")?;
	connection.pragma_update(None, "journal_mode", "wal")?;
	connection.pragma_update(None, "mmap_size", "2147483648")?;
	connection.pragma_update(None, "recursive_triggers", "on")?;
	connection.pragma_update(None, "synchronous", "off")?;
	connection.pragma_update(None, "temp_store", "memory")?;
	Ok(())
}

pub async fn migrate(database: &db::sqlite::Database) -> tg::Result<()> {
	let migrations = vec![migration_0000(database).boxed()];

	let connection = database
		.connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	let version = connection
		.with(|connection, _cache| {
			connection
				.pragma_query_value(None, "user_version", |row| {
					Ok(row.get_unwrap::<_, i64>(0).to_usize().unwrap())
				})
				.map_err(|source| tg::error!(!source, "failed to get the version"))
		})
		.await?;
	drop(connection);

	// If this path is from a newer version of Tangram, then return an error.
	if version > migrations.len() {
		return Err(tg::error!(
			r"The index has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Run all migrations and update the version.
	let migrations = migrations.into_iter().enumerate().skip(version);
	for (version, migration) in migrations {
		// Run the migration.
		migration.await?;

		// Update the version.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		connection
			.with(move |connection, _cache| {
				connection
					.pragma_update(None, "user_version", (version + 1).to_i64().unwrap())
					.map_err(|source| tg::error!(!source, "failed to get the version"))
			})
			.await?;
	}

	Ok(())
}

async fn migration_0000(database: &db::sqlite::Database) -> tg::Result<()> {
	let sql = SQL;
	let connection = database
		.write_connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	connection
		.with(move |connection, _cache| {
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
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

	async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		self.touch_object(id).await
	}

	async fn touch_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		self.touch_process(id).await
	}

	async fn clean(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		self.clean(max_touched_at, n).await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
	}
}
