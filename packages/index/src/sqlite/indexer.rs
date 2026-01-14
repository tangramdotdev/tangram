use {
	super::Index,
	crate::{
		DeleteTagArg, PutCacheEntryArg, PutObjectArg, PutProcessArg, PutTagArg, TouchObjectArg,
		TouchProcessArg,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Index {
	#[expect(clippy::too_many_arguments)]
	pub async fn handle_messages(
		&self,
		put_cache_entry_messages: Vec<PutCacheEntryArg>,
		put_object_messages: Vec<PutObjectArg>,
		touch_object_messages: Vec<TouchObjectArg>,
		put_process_messages: Vec<PutProcessArg>,
		touch_process_messages: Vec<TouchProcessArg>,
		put_tag_messages: Vec<PutTagArg>,
		delete_tag_messages: Vec<DeleteTagArg>,
	) -> tg::Result<()> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with(move |connection, cache| {
				// Begin a transaction.
				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				// Handle the messages.
				super::indexer_put_cache_entries(&transaction, cache, put_cache_entry_messages)?;
				super::indexer_put_objects(&transaction, cache, put_object_messages)?;
				super::indexer_touch_objects(&transaction, cache, touch_object_messages)?;
				super::indexer_put_processes(&transaction, cache, put_process_messages)?;
				super::indexer_touch_processes(&transaction, cache, touch_process_messages)?;
				super::indexer_put_tags(&transaction, cache, put_tag_messages)?;
				super::indexer_delete_tags(&transaction, cache, delete_tag_messages)?;
				super::indexer_increment_transaction_id(&transaction, cache)?;

				// Commit the transaction.
				transaction
					.commit()
					.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

				Ok::<_, tg::Error>(())
			})
			.await?;

		Ok(())
	}
}
