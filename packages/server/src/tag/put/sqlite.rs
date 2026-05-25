use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
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
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		connection
			.with({
				let tag = tag.clone();
				let arg = arg.clone();
				let created_by = created_by.clone();
				move |connection, cache| {
					let transaction = connection
						.transaction()
						.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
					Self::put_tag_sqlite_sync(
						&transaction,
						cache,
						&tag,
						&arg,
						created_by.as_ref(),
						grant_creator_admin,
					)?;
					transaction
						.commit()
						.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.await?;

		Ok(())
	}

	pub(crate) fn put_tag_sqlite_sync(
		transaction: &sqlite::Transaction,
		_cache: &db::sqlite::Cache,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: bool,
	) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("cannot put an empty tag"));
		}

		let namespace = Self::get_or_create_namespace_sqlite_sync(transaction, &tag.namespace)?;
		let statement = indoc!(
			"
				insert into tags (namespace, name, item)
				values (?1, ?2, ?3)
				on conflict (namespace, name) do update
				set item = excluded.item
				where ?4 or tags.item = excluded.item ;
			"
		);
		let n = transaction
			.execute(
				statement,
				sqlite::params![
					namespace,
					tag.name.to_string(),
					arg.item.to_string(),
					arg.force
				],
			)
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
			let n = transaction
				.execute(
					statement,
					sqlite::params![
						namespace,
						tag.name.to_string(),
						user.to_string(),
						created_at
					],
				)
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if n > 0 {
				Self::increment_namespace_visibility_for_user_sqlite_sync(
					transaction,
					&tag.namespace,
					user,
				)?;
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
			let n = transaction
				.execute(
					statement,
					sqlite::params![namespace, tag.name.to_string(), created_at, created_by],
				)
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if n > 0 {
				Self::increment_namespace_visibility_for_all_sqlite_sync(
					transaction,
					&tag.namespace,
				)?;
			}
		}

		Ok(())
	}
}
