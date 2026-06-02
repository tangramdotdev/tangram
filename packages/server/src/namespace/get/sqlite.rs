use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::ops::ControlFlow,
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
				let output = Self::try_get_namespace_sqlite_sync(&transaction, cache, &namespace)?
					.map(|_| tg::namespace::get::Output { namespace });
				Ok(output)
			})
			.await
	}

	pub(crate) fn try_get_namespace_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<i64>> {
		match Self::try_get_namespace_sqlite_sync_retry(transaction, cache, namespace)? {
			ControlFlow::Break(id) => Ok(id),
			ControlFlow::Continue(error) => Err(tg::error!(!error, "database error")),
		}
	}

	pub(crate) fn try_get_namespace_sqlite_sync_retry(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<Option<i64>, db::sqlite::Error>> {
		if namespace.is_root() {
			return Ok(ControlFlow::Break(Some(0)));
		}
		let statement = indoc!(
			"
				select id
				from namespaces
				where name = ?1;
			"
		);
		let result = cache.get(transaction, statement.into());
		let mut statement = crate::database::retry!(result, "failed to prepare the statement");
		let params = sqlite::params![namespace.to_string()];
		let result = statement.query(params).map_err(db::sqlite::Error::from);
		let mut rows = crate::database::retry!(result, "failed to execute the statement");
		let result = rows.next().map_err(db::sqlite::Error::from);
		let Some(row) = crate::database::retry!(result, "failed to get the next row") else {
			return Ok(ControlFlow::Break(None));
		};
		let result = row.get(0).map_err(db::sqlite::Error::from);
		let id = crate::database::retry!(result, "failed to get the id column");
		Ok(ControlFlow::Break(Some(id)))
	}
}
