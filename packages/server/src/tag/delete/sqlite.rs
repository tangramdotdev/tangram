use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn delete_tags_sqlite(
		&self,
		database: &db::sqlite::Database,
		pattern: &tg::specifier::Pattern,
		recursive: bool,
	) -> tg::Result<tg::tag::delete::Output> {
		let pattern = pattern.clone();
		database
			.run(move |transaction, cache| {
				Self::delete_tag_sqlite_sync(transaction, cache, &pattern, recursive)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the tags"))
	}

	pub(crate) fn delete_tag_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::specifier::Pattern,
		recursive: bool,
	) -> tg::Result<ControlFlow<tg::tag::delete::Output, db::sqlite::Error>> {
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
			let namespace_id = match Self::try_get_namespace_sqlite_sync_retry(
				transaction,
				cache,
				&m.tag.namespace,
			)? {
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let Some(namespace_id) = namespace_id else {
				continue;
			};
			let statement = indoc!(
				"
					delete from tags
					where namespace = ?1 and name = ?2;
				"
			);
			let result = transaction
				.execute(
					statement,
					sqlite::params![namespace_id, m.tag.name.to_string()],
				)
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
			let statement = indoc!(
				r"
					select principal, permission
					from tag_grants
					where namespace = ?1 and name = ?2;
				"
			);
			let result = transaction
				.prepare(statement)
				.map_err(db::sqlite::Error::from);
			let mut statement = crate::database::retry!(result, "failed to prepare the statement");
			let mut rows = {
				let result = statement
					.query(sqlite::params![namespace_id, m.tag.name.to_string()])
					.map_err(db::sqlite::Error::from);
				crate::database::retry!(result, "failed to execute the statement")
			};
			loop {
				let result = rows.next().map_err(db::sqlite::Error::from);
				let Some(row) = crate::database::retry!(result, "failed to get the next row")
				else {
					break;
				};
				let result = row.get::<_, String>(0).map_err(db::sqlite::Error::from);
				let principal =
					crate::database::retry!(result, "failed to get the principal column");
				let result = row.get::<_, String>(1).map_err(db::sqlite::Error::from);
				let permission =
					crate::database::retry!(result, "failed to get the permission column")
						.parse::<tg::Permission>()
						.map_err(|error| tg::error!(!error, "invalid permission"))?;
				if permission.implies(tg::Permission::Read) {
					match Self::decrement_namespace_visibility_for_grant_sqlite_sync(
						transaction,
						&m.tag.namespace,
						Some(principal.as_str()),
						None,
						false,
					)? {
						ControlFlow::Break(()) => {},
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				}
			}
			drop(rows);
			drop(statement);
			let statement = indoc!(
				"
					delete from tag_grants
					where namespace = ?1 and name = ?2;
				"
			);
			let result = transaction
				.execute(
					statement,
					sqlite::params![namespace_id, m.tag.name.to_string()],
				)
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
			deleted.push(m.tag);
		}

		Ok(ControlFlow::Break(tg::tag::delete::Output { deleted }))
	}
}
