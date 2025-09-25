use {crate::Server, indoc::indoc, tangram_client as tg, tangram_database::prelude::*};

impl Server {
	pub(crate) async fn delete_tag_postgres(
		database: &tangram_database::postgres::Database,
		tag: &tg::Tag,
	) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("cannot delete an empty tag"));
		}
		let mut connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Find the tag by traversing the component path.
		let mut parent: i64 = 0;
		for component in tag.components() {
			let statement = indoc!(
				"
					select id, item
					from tags
					where parent = $1 and component = $2;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&parent, &component.to_string()])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			let row = rows
				.first()
				.ok_or_else(|| tg::error!(%tag, "tag not found"))?;

			let id: i64 = row.get(0);
			parent = id;
		}

		// Delete the tag if it is a leaf.
		let statement = indoc!(
			"
				delete
				from tags
				where id = $1
				and item is not null
				returning id;
			"
		);
		let rows = transaction
			.inner()
			.query(statement, &[&parent])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		rows.first()
			.ok_or_else(|| tg::error!(%tag, "expected a leaf tag"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}
}
