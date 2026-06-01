use {
	crate::Session,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn delete_tags_postgres(
		&self,
		database: &db::postgres::Database,
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
		db::postgres::run!(database, |transaction| {
			self.delete_tags_postgres_with_transaction(transaction, pattern, recursive)
				.await
		})
		.map_err(|error| tg::error!(!error, "failed to delete the tags"))
	}

	async fn delete_tags_postgres_with_transaction(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<ControlFlow<tg::tag::delete::Output, db::postgres::Error>> {
		let mut matches = if recursive {
			self.match_tags_for_list_postgres(transaction, pattern)
				.await?
		} else {
			self.list_tag_matches_for_list_postgres(transaction, pattern)
				.await?
		};
		matches.sort_by(|a, b| a.tag.cmp(&b.tag));
		let mut deleted = Vec::new();
		for m in matches {
			let Some(namespace_id) =
				Self::try_get_namespace_postgres_with_transaction(transaction, &m.tag.namespace)
					.await?
			else {
				continue;
			};
			let statement = indoc!(
				"
					delete from tags
					where namespace = $1 and name = $2;
				"
			);
			let result = transaction
				.inner()
				.execute(statement, &[&namespace_id, &m.tag.name.to_string()])
				.await
				.map_err(db::postgres::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
			let statement = indoc!(
				r"
					select principal, permission
					from tag_grants
					where namespace = $1 and name = $2;
				"
			);
			let result = transaction
				.inner()
				.query(statement, &[&namespace_id, &m.tag.name.to_string()])
				.await
				.map_err(db::postgres::Error::from);
			let rows = crate::database::retry!(result, "failed to execute the statement");
			for row in rows {
				let principal = row
					.try_get::<_, String>(0)
					.map_err(|error| tg::error!(!error, "failed to get the principal column"))?;
				let permission = row
					.try_get::<_, String>(1)
					.map_err(|error| tg::error!(!error, "failed to get the permission column"))?
					.parse::<tg::Permission>()
					.map_err(|error| tg::error!(!error, "invalid permission"))?;
				if permission.implies(tg::Permission::Read) {
					match Self::decrement_namespace_visibility_for_grant_postgres(
						transaction,
						&m.tag.namespace,
						Some(principal.as_str()),
						None,
						false,
					)
					.await?
					{
						ControlFlow::Break(()) => {},
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				}
			}
			let statement = indoc!(
				"
					delete from tag_grants
					where namespace = $1 and name = $2;
				"
			);
			let result = transaction
				.inner()
				.execute(statement, &[&namespace_id, &m.tag.name.to_string()])
				.await
				.map_err(db::postgres::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
			deleted.push(m.tag);
		}
		Ok(ControlFlow::Break(tg::tag::delete::Output { deleted }))
	}
}
