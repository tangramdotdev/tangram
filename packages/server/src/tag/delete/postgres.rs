use {crate::Server, indoc::indoc, tangram_client as tg, tangram_database::prelude::*};

impl Server {
	pub(crate) async fn delete_tag_postgres(
		database: &tangram_database::postgres::Database,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<tg::tag::delete::Output> {
		if pattern.is_empty() {
			return Err(tg::error!("cannot delete an empty pattern"));
		}

		// Check if pattern contains wildcards or version operators.
		for component in pattern.components() {
			if component == "*" || component.contains(['=', '>', '<', '^']) {
				return Err(tg::error!(
					"pattern matching is not yet supported for postgres, only exact tag paths"
				));
			}
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
		for component in pattern.components() {
			let statement = indoc!(
				"
					select id, item
					from tags
					where parent = $1 and component = $2;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&parent, &component])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			let row = rows
				.first()
				.ok_or_else(|| tg::error!("tag not found: {}", pattern))?;

			let id: i64 = row.get(0);
			parent = id;
		}

		// Check if the tag is a leaf or an empty branch.
		let statement = indoc!(
			"
				select item, (select count(*) from tags where parent = $1) as child_count
				from tags
				where id = $1;
			"
		);
		let rows = transaction
			.inner()
			.query(statement, &[&parent])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let row = rows
			.first()
			.ok_or_else(|| tg::error!("tag not found: {}", pattern))?;

		let item: Option<String> = row.get(0);
		let child_count: i64 = row.get(1);

		if item.is_none() && child_count > 0 {
			return Err(tg::error!(
				"cannot delete branch tag {} with children",
				pattern
			));
		}

		let is_leaf = item.is_some();

		// Delete the tag.
		let statement = indoc!(
			"
				delete from tags
				where id = $1
				returning id;
			"
		);
		let rows = transaction
			.inner()
			.query(statement, &[&parent])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		rows.first()
			.ok_or_else(|| tg::error!("failed to delete tag: {}", pattern))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Convert pattern to tag (since we only support exact paths in postgres).
		let tag = pattern.as_str().parse()?;
		let leaf_deleted = if is_leaf { vec![tag.clone()] } else { vec![] };
		let output = tg::tag::delete::Output {
			deleted: vec![tag],
			leaf_deleted,
		};
		Ok(output)
	}
}
