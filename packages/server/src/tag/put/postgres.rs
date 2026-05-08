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
	) -> tg::Result<()> {
		let mut connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let mut transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::put_tag_postgres_inner(&mut transaction, tag, arg)
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
		Ok(())
	}
}
