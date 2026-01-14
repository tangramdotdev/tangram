use {
	super::Index,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Index {
	pub async fn handle_queue(&self, batch_size: usize) -> tg::Result<usize> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let n = connection
			.with(move |connection, cache| {
				// Begin a transaction.
				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				let mut n = batch_size;
				n -= super::indexer_handle_stored_object(&transaction, cache, n)?;
				n -= super::indexer_handle_stored_process(&transaction, cache, n)?;
				n -= super::indexer_handle_reference_count_cache_entry(&transaction, cache, n)?;
				n -= super::indexer_handle_reference_count_object(&transaction, cache, n)?;
				n -= super::indexer_handle_reference_count_process(&transaction, cache, n)?;
				let n = batch_size - n;

				// Commit the transaction.
				if n > 0 {
					super::indexer_increment_transaction_id(&transaction, cache)?;
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
				}

				Ok::<_, tg::Error>(n)
			})
			.await?;

		Ok(n)
	}
}
