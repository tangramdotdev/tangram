use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn put_tag_sqlite(
		&self,
		database: &db::sqlite::Database,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
		grant_creator_admin: bool,
	) -> tg::Result<()> {
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());
		db::sqlite::run!(
			database,
			[
				tag = tag.clone(),
				arg = arg.clone(),
				created_by = created_by.clone()
			],
			|transaction, cache| {
				Self::put_tag_sqlite_sync(
					transaction,
					cache,
					&tag,
					&arg,
					created_by.as_ref(),
					grant_creator_admin,
				)
			}
		)
		.map_err(|error| tg::error!(!error, "failed to put the tag"))
	}

	pub(crate) fn put_tag_sqlite_sync(
		transaction: &sqlite::Transaction,
		_cache: &db::sqlite::Cache,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: bool,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
		if tag.is_empty() {
			return Err(tg::error!("cannot put an empty tag"));
		}

		let namespace =
			match Self::get_or_create_namespace_sqlite_sync(transaction, &tag.namespace)? {
				ControlFlow::Break(namespace) => namespace,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let statement = indoc!(
			"
				insert into tags (namespace, name, item)
				values (?1, ?2, ?3)
				on conflict (namespace, name) do update
				set item = excluded.item
				where ?4 or tags.item = excluded.item;
			"
		);
		let result = transaction
			.execute(
				statement,
				sqlite::params![
					namespace,
					tag.name.to_string(),
					arg.item.to_string(),
					arg.force
				],
			)
			.map_err(db::sqlite::Error::from);
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			return Err(tg::error!("the tag already exists with a different item"));
		}
		if grant_creator_admin && let Some(user) = created_by {
			let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
			let statement = indoc!(
				r"
					insert into tag_grants (namespace, name, principal, permission, created_at, created_by)
					select ?1, ?2, ?3, 'admin', ?4, ?3
					where not exists (
						select 1
						from tag_grants
						where namespace = ?1 and name = ?2 and principal = ?3 and permission = 'admin'
					);
				"
			);
			let result = transaction
				.execute(
					statement,
					sqlite::params![
						namespace,
						tag.name.to_string(),
						user.to_string(),
						created_at
					],
				)
				.map_err(db::sqlite::Error::from);
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n > 0 {
				match Self::increment_namespace_visibility_for_user_sqlite_sync(
					transaction,
					&tag.namespace,
					user,
				)? {
					ControlFlow::Break(()) => {},
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
		}
		if arg.all && !arg.replicate {
			let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
			let created_by = created_by.map(ToString::to_string);
			let statement = indoc!(
				r"
					insert into tag_grants (namespace, name, principal, permission, created_at, created_by)
					select ?1, ?2, 'all', 'read', ?3, ?4
					where not exists (
						select 1
						from tag_grants
						where namespace = ?1 and name = ?2 and principal = 'all' and permission = 'read'
					);
				"
			);
			let result = transaction
				.execute(
					statement,
					sqlite::params![namespace, tag.name.to_string(), created_at, created_by],
				)
				.map_err(db::sqlite::Error::from);
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n > 0 {
				match Self::increment_namespace_visibility_for_all_sqlite_sync(
					transaction,
					&tag.namespace,
				)? {
					ControlFlow::Break(()) => {},
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
		}

		Ok(ControlFlow::Break(()))
	}
}
