use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn post_tag_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		arg: &tg::tag::batch::Arg,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		connection
			.with({
				let arg = arg.clone();
				move |connection, cache| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

					// Insert the tags.
					for tg::tag::batch::Item { tag, item, force } in &arg.tags {
						let arg = tg::tag::put::Arg {
							force: *force,
							item: item.clone(),
							location: None,
							replicate: false,
							tag: None,
						};
						Self::put_tag_sqlite_sync(&transaction, cache, tag, &arg)
							.map_err(|error| tg::error!(!error, %tag, "failed to put tag"))?;
					}
					// Commit the transaction.
					transaction
						.commit()
						.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

					Ok::<_, tg::Error>(())
				}
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put tags"))?;

		Ok(())
	}
}
