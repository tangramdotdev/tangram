use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_get_namespace_sqlite(
		&self,
		database: &db::sqlite::Database,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<tg::namespace::get::Output>> {
		if namespace.is_root() {
			return Ok(Some(tg::namespace::get::Output {
				namespace: namespace.clone(),
			}));
		}
		let connection = database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let namespace = namespace.clone();
		connection
			.with(move |connection, cache| {
				let transaction = connection
					.transaction()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let output = Self::get_namespace_sqlite_sync(&transaction, cache, &namespace)?
					.map(|_| tg::namespace::get::Output { namespace });
				Ok(output)
			})
			.await
	}

	pub(crate) fn get_namespace_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<i64>> {
		if namespace.is_root() {
			return Ok(Some(0));
		}
		let statement = indoc!(
			"
				select id
				from namespaces
				where name = ?1 ;
			"
		);
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let params = sqlite::params![namespace.to_string()];
		let mut rows = statement
			.query(params)
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to get the next row"))?
		else {
			return Ok(None);
		};
		let id = row
			.get(0)
			.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
		Ok(Some(id))
	}
}
