use {
	crate::Session,
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn delete_tags_turso(
		&self,
		database: &db::turso::Database,
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
		let pattern = pattern.clone();
		let session = self.clone();
		database
			.run(|transaction| {
				let pattern = pattern.clone();
				let session = session.clone();
				async move {
					session
						.delete_tags_turso_with_transaction(transaction, &pattern, recursive)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the tags"))
	}

	async fn delete_tags_turso_with_transaction(
		&self,
		transaction: &db::turso::Transaction<'_>,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<ControlFlow<tg::tag::delete::Output, db::turso::Error>> {
		let mut matches = if recursive {
			Self::match_tags_for_list_turso_with_transaction(transaction, pattern).await?
		} else {
			Self::list_tag_matches_for_list_turso_with_transaction(transaction, pattern).await?
		};
		matches.sort_by(|a, b| a.tag.cmp(&b.tag));
		let mut deleted = Vec::new();
		for m in matches {
			let Some(namespace_id) =
				Self::try_get_namespace_turso_with_transaction(transaction, &m.tag.namespace)
					.await?
			else {
				continue;
			};
			let result = transaction
				.execute(
					"delete from tags where namespace = ?1 and name = ?2;".into(),
					db::params![namespace_id, m.tag.name.to_string()],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			#[derive(db::row::Deserialize)]
			struct GrantRow {
				principal: String,
				#[tangram_database(as = "db::value::FromStr")]
				permission: tg::Permission,
			}
			let result = transaction
				.query_all_into::<GrantRow>(
					indoc!(
						r"
							select principal, permission
							from tag_grants
							where namespace = ?1 and name = ?2;
						"
					)
					.into(),
					db::params![namespace_id, m.tag.name.to_string()],
				)
				.await;
			let rows = crate::database::retry!(result, "failed to execute the statement");
			for row in rows {
				if row.permission.implies(tg::Permission::Read) {
					match Self::decrement_namespace_visibility_for_grant_turso_with_transaction(
						transaction,
						&m.tag.namespace,
						Some(row.principal.as_str()),
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
			let result = transaction
				.execute(
					"delete from tag_grants where namespace = ?1 and name = ?2;".into(),
					db::params![namespace_id, m.tag.name.to_string()],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			deleted.push(m.tag);
		}
		Ok(ControlFlow::Break(tg::tag::delete::Output { deleted }))
	}
}
