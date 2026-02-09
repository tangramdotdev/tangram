use {
	crate::Server,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn post_tag_batch_sqlite(
		database: &db::sqlite::Database,
		arg: &tg::tag::batch::Arg,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with({
				let arg = arg.clone();
				move |connection, cache| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Insert the tags.
					for tg::tag::batch::Item { tag, item, force } in &arg.tags {
						let arg = tg::tag::put::Arg {
							force: *force,
							item: item.clone(),
							local: None,
							remotes: None,
						};
						Self::put_tag_sqlite_sync(&transaction, cache, tag, &arg)
							.map_err(|source| tg::error!(!source, %tag, "failed to put tag"))?;
					}
					// Commit the transaction.
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;

					Ok::<_, tg::Error>(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to put tags"))?;

		Ok(())
	}
}
