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
	) -> tg::Result<()> {
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		connection
			.with({
				let tag = tag.clone();
				let arg = arg.clone();
				move |connection, cache| {
					let transaction = connection
						.transaction()
						.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
					Self::put_tag_sqlite_sync(&transaction, cache, &tag, &arg)?;
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
				set item = excluded.item ;
			"
		);
		transaction
			.execute(
				statement,
				sqlite::params![namespace, tag.name.to_string(), arg.item.to_string()],
			)
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Ok(())
	}
}
