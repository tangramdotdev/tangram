use {
	crate::Server,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn post_tags_batch_postgres(
		database: &db::postgres::Database,
		arg: &tg::tag::post::Arg,
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let mut transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		for tg::tag::post::Item { tag, item, force } in &arg.tags {
			let arg = tg::tag::put::Arg {
				force: *force,
				item: item.clone(),
				remote: None,
			};
			Self::put_tag_postgres_inner(&mut transaction, tag, &arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to perform the transaction"))?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}
}
