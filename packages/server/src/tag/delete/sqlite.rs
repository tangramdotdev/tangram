use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn delete_tag_sqlite(
		database: &db::sqlite::Database,
		pattern: &tg::tag::Pattern,
		recursive: bool,
	) -> tg::Result<tg::tag::delete::Output> {
		// Get a database connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let output = connection
			.with({
				let pattern = pattern.clone();
				move |connection, cache| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Delete tags matching the pattern.
					let output =
						Self::delete_tag_sqlite_sync(&transaction, cache, &pattern, recursive)?;

					// Commit the transaction.
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;

					Ok::<_, tg::Error>(output)
				}
			})
			.await?;

		Ok(output)
	}

	pub(crate) fn delete_tag_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::tag::Pattern,
		recursive: bool,
	) -> tg::Result<tg::tag::delete::Output> {
		if pattern.is_empty() {
			return Err(tg::error!("cannot delete an empty pattern"));
		}

		// Get all tags matching the pattern.
		let mut matches = Self::match_tags_sqlite_sync(transaction, cache, pattern, recursive)?;

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
						where child = ?1;
					"
				);
				let mut statement = cache
					.get(transaction, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![m.id.to_i64().unwrap()];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				let statement = indoc!(
					"
						delete from tags
						where id = ?1;
					"
				);
				let mut statement = cache
					.get(transaction, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![m.id.to_i64().unwrap()];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				deleted.push(m.tag);
			} else {
				// This is a branch tag.
				let statement = indoc!(
					"
						select count(*) from tag_children
						where tag = ?1;
					"
				);
				let mut statement = cache
					.get(transaction, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![m.id.to_i64().unwrap()];
				let count: i64 = statement
					.query_row(params, |row| row.get(0))
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

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
						where child = ?1;
					"
				);
				let mut statement = cache
					.get(transaction, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![m.id.to_i64().unwrap()];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				let statement = indoc!(
					"
						delete from tags
						where id = ?1;
					"
				);
				let mut statement = cache
					.get(transaction, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![m.id.to_i64().unwrap()];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				deleted.push(m.tag);
			}
		}

		let output = tg::tag::delete::Output { deleted };
		Ok(output)
	}
}
