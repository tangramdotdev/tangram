use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_delete_namespace_postgres(
		&self,
		database: &db::postgres::Database,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<()>> {
		if namespace.is_root() {
			return Err(tg::error!("cannot delete the root namespace"));
		}
		let mut connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(id) = Self::get_namespace_postgres(&transaction, namespace).await? else {
			return Ok(None);
		};
		let rows = transaction
			.inner()
			.query("select count(*) from namespaces where parent = $1;", &[&id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let children: i64 = rows
			.first()
			.ok_or_else(|| tg::error!("expected a row"))?
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the count column"))?;
		let rows = transaction
			.inner()
			.query("select count(*) from tags where namespace = $1;", &[&id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let tags: i64 = rows
			.first()
			.ok_or_else(|| tg::error!("expected a row"))?
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the count column"))?;
		if children > 0 || tags > 0 {
			return Err(tg::error!("namespace is not empty"));
		}
		transaction
			.inner()
			.execute("delete from namespaces where id = $1;", &[&id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(Some(()))
	}
}
