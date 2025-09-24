use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite, tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn delete_tag_sqlite(
		database: &db::sqlite::Database,
		tag: &tg::Tag,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with({
				let tag = tag.clone();
				move |connection| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Insert the tag.
					Self::delete_tag_sqlite_sync(&transaction, &tag)?;

					// Commit the transaction.
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;

					Ok::<_, tg::Error>(())
				}
			})
			.await?;

		Ok(())
	}

	pub(crate) fn delete_tag_sqlite_sync(
		transaction: &sqlite::Transaction,
		tag: &tg::Tag,
	) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("cannot delete an empty tag"));
		}

		// Find the tag by traversing the component path.
		let mut parent = 0;
		for component in tag.components().iter() {
			let statement = indoc!(
				"
					select id, item
					from tags
					where parent = ?1 and component = ?2;
				"
			);
			let mut statement = transaction
				.prepare_cached(statement)
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent, component.to_string()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			let row = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.ok_or_else(|| tg::error!(%tag, "tag not found"))?;

			let id = row.get_unwrap::<_, u64>(0);
			parent = id;
		}

		// Delete the tag if it is a leaf.
		let statement = indoc!(
			"
				delete
				from tags
				where id = ?1
				and item is not null
				returning id;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![parent];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		rows.next()
			.map_err(|source| tg::error!(!source, "failed to execute the query"))?
			.ok_or_else(|| tg::error!(%tag, "expected a leaf tag"))?;

		Ok(())
	}
}
