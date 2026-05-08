use {
	crate::Session,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_get_namespace_postgres(
		&self,
		database: &db::postgres::Database,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<tg::namespace::get::Output>> {
		if namespace.is_root() {
			return Ok(Some(tg::namespace::get::Output {
				namespace: namespace.clone(),
			}));
		}
		let mut connection = database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let output = Self::get_namespace_postgres(&transaction, namespace)
			.await?
			.map(|_| tg::namespace::get::Output {
				namespace: namespace.clone(),
			});
		Ok(output)
	}

	pub(crate) async fn get_namespace_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<i64>> {
		if namespace.is_root() {
			return Ok(Some(0));
		}
		let statement = indoc!(
			"
				select id
				from namespaces
				where name = $1 ;
			"
		);
		let rows = transaction
			.inner()
			.query(statement, &[&namespace.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		let id = row
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
		Ok(Some(id))
	}
}
