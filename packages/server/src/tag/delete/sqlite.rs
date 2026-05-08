use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn delete_tags_sqlite(
		&self,
		database: &db::sqlite::Database,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<tg::tag::delete::Output> {
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		let output = connection
			.with({
				let pattern = pattern.clone();
				move |connection, cache| {
					let transaction = connection
						.transaction()
						.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
					let output =
						Self::delete_tag_sqlite_sync(&transaction, cache, &pattern, recursive)?;
					transaction
						.commit()
						.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
					Ok::<_, tg::Error>(output)
				}
			})
			.await?;

		Ok(output)
	}

	pub(crate) fn delete_tag_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<tg::tag::delete::Output> {
		if pattern.is_empty() {
			return Err(tg::error!("cannot delete an empty pattern"));
		}
		if !recursive && pattern.contains_operators() {
			return Err(tg::error!(
				"cannot delete multiple tags without --recursive"
			));
		}

		let mut matches = if recursive {
			Self::match_tags_for_list_sqlite_sync(transaction, cache, pattern)?
		} else {
			Self::list_tag_matches_for_list_sqlite_sync(transaction, cache, pattern)?
		};
		matches.sort_by(|a, b| a.tag.cmp(&b.tag));
		let mut deleted = Vec::new();
		for m in matches {
			let Some(namespace_id) =
				Self::get_namespace_sqlite_sync(transaction, cache, &m.tag.namespace)?
			else {
				continue;
			};
			let statement = indoc!(
				"
					delete from tags
					where namespace = ?1 and name = ?2 ;
				"
			);
			transaction
				.execute(
					statement,
					sqlite::params![namespace_id, m.tag.name.to_string()],
				)
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			deleted.push(m.tag);
		}

		Ok(tg::tag::delete::Output { deleted })
	}
}
