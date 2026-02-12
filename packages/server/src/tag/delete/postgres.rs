use {
	crate::Server, indoc::indoc, num::ToPrimitive as _, tangram_client::prelude::*,
	tangram_database::prelude::*,
};

impl Server {
	pub(crate) async fn delete_tag_postgres(
		database: &tangram_database::postgres::Database,
		pattern: &tg::tag::Pattern,
		recursive: bool,
	) -> tg::Result<tg::tag::delete::Output> {
		if pattern.is_empty() {
			return Err(tg::error!("cannot delete an empty pattern"));
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

		// Get all tags matching the pattern.
		let mut matches = Self::match_tags_postgres(&transaction, pattern, recursive).await?;

		// Sort by tag length descending to delete leaves before branches.
		matches.sort_by_key(|m| std::cmp::Reverse(m.tag.as_str().len()));

		// Validate and delete each match.
		let mut deleted = Vec::new();
		for m in matches {
			let is_leaf = m.item.is_some();
			if is_leaf {
				// This is a leaf tag, safe to delete.
				let statement = indoc!(
					"
						delete from tag_children
						where child = $1;
					"
				);
				transaction
					.inner()
					.execute(statement, &[&m.id.to_i64().unwrap()])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				let statement = indoc!(
					"
						delete from tags
						where id = $1;
					"
				);
				transaction
					.inner()
					.execute(statement, &[&m.id.to_i64().unwrap()])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				deleted.push(m.tag);
			} else {
				// This is a branch tag.
				let statement = indoc!(
					"
						select count(*) from tag_children
						where tag = $1;
					"
				);
				let rows = transaction
					.inner()
					.query(statement, &[&m.id.to_i64().unwrap()])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let count: i64 = rows
					.first()
					.ok_or_else(|| tg::error!("failed to get count"))?
					.get(0);

				if count > 0 {
					return Err(tg::error!(
						"cannot delete branch tag {} with children",
						m.tag
					));
				}

				// No children, safe to delete.
				let statement = indoc!(
					"
						delete from tag_children
						where child = $1;
					"
				);
				transaction
					.inner()
					.execute(statement, &[&m.id.to_i64().unwrap()])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				let statement = indoc!(
					"
						delete from tags
						where id = $1;
					"
				);
				transaction
					.inner()
					.execute(statement, &[&m.id.to_i64().unwrap()])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				deleted.push(m.tag);
			}
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let output = tg::tag::delete::Output { deleted };

		Ok(output)
	}
}
