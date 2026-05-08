use {
	crate::Session,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_delete_namespace_sqlite(
		&self,
		database: &db::sqlite::Database,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<()>> {
		if namespace.is_root() {
			return Err(tg::error!("cannot delete the root namespace"));
		}
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let namespace = namespace.clone();
		connection
			.with(move |connection, cache| {
				let transaction = connection
					.transaction()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let Some(id) = Self::get_namespace_sqlite_sync(&transaction, cache, &namespace)?
				else {
					return Ok(None);
				};
				let children = transaction
					.query_row(
						"select count(*) from namespaces where parent = ?1;",
						sqlite::params![id],
						|row| row.get::<_, i64>(0),
					)
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				let tags = transaction
					.query_row(
						"select count(*) from tags where namespace = ?1;",
						sqlite::params![id],
						|row| row.get::<_, i64>(0),
					)
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				if children > 0 || tags > 0 {
					return Err(tg::error!("namespace is not empty"));
				}
				transaction
					.execute("delete from namespaces where id = ?1;", sqlite::params![id])
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				transaction
					.commit()
					.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
				Ok(Some(()))
			})
			.await
	}
}
