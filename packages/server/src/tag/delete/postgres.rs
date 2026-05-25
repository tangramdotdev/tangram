use {crate::Session, indoc::indoc, tangram_client::prelude::*, tangram_database::prelude::*};

impl Session {
	pub(crate) async fn delete_tags_postgres(
		&self,
		database: &tangram_database::postgres::Database,
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

		let mut connection = database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let mut matches = if recursive {
			self.match_tags_for_list_postgres(&transaction, pattern)
				.await?
		} else {
			self.list_tag_matches_for_list_postgres(&transaction, pattern)
				.await?
		};
		matches.sort_by(|a, b| a.tag.cmp(&b.tag));
		let mut deleted = Vec::new();
		for m in matches {
			let Some(namespace_id) =
				Self::get_namespace_postgres(&transaction, &m.tag.namespace).await?
			else {
				continue;
			};
			let statement = indoc!(
				"
					delete from tags
					where namespace = $1 and name = $2 ;
				"
			);
			transaction
				.inner()
				.execute(statement, &[&namespace_id, &m.tag.name.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			let statement = indoc!(
				r"
					select principal, permission
					from tag_grants
					where namespace = $1 and name = $2 ;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&namespace_id, &m.tag.name.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
					Self::decrement_namespace_visibility_for_grant_postgres(
						&transaction,
						&m.tag.namespace,
						Some(principal.as_str()),
						None,
						false,
					)
					.await?;
				}
			}
			let statement = indoc!(
				"
					delete from tag_grants
					where namespace = $1 and name = $2 ;
				"
			);
			transaction
				.inner()
				.execute(statement, &[&namespace_id, &m.tag.name.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			deleted.push(m.tag);
		}
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(tg::tag::delete::Output { deleted })
	}
}
