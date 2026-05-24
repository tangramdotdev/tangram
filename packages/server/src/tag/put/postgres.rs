use {
	crate::Session,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn put_tag_postgres(
		&self,
		database: &db::postgres::Database,
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
		let mut connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let mut transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::put_tag_postgres_inner(
			&mut transaction,
			tag,
			arg,
			created_by.as_ref(),
			grant_creator_admin,
		)
		.await
		.map_err(|error| tg::error!(!error, "failed to perform the transaction"))?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(crate) async fn put_tag_postgres_inner(
		transaction: &mut db::postgres::Transaction<'_>,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
		created_by: Option<&tg::user::Id>,
		grant_creator_admin: bool,
	) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("cannot put an empty tag"));
		}

		let namespace = Self::get_or_create_namespace_postgres(transaction, &tag.namespace).await?;
		let statement = indoc!(
			"
				insert into tags (namespace, name, item)
				values ($1, $2, $3)
				on conflict (namespace, name) do update
				set item = excluded.item ;
			"
		);
		transaction
			.inner()
			.execute(
				statement,
				&[&namespace, &tag.name.to_string(), &arg.item.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if grant_creator_admin && let Some(user) = created_by {
			let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
			let statement = indoc!(
				r#"
					insert into tag_grants (namespace, name, "user", permission, created_at, created_by)
					select $1, $2, $3, 'admin', $4, $3
					where not exists (
						select 1
						from tag_grants
						where namespace = $1 and name = $2 and "user" = $3 and permission = 'admin'
					);
				"#
			);
			let user = user.to_string();
			transaction
				.inner()
				.execute(
					statement,
					&[&namespace, &tag.name.to_string(), &user, &created_at],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		if arg.all && !arg.replicate {
			let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
			let created_by = created_by.map(ToString::to_string);
			let statement = indoc!(
				r#"
					insert into tag_grants (namespace, name, "all", permission, created_at, created_by)
					select $1, $2, true, 'read', $3, $4
					where not exists (
						select 1
						from tag_grants
						where namespace = $1 and name = $2 and "all" and permission = 'read'
					);
				"#
			);
			transaction
				.inner()
				.execute(
					statement,
					&[&namespace, &tag.name.to_string(), &created_at, &created_by],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(())
	}
}
