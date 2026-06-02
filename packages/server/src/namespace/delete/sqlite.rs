use {
	crate::Session,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
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
		let namespace = namespace.clone();
		database
			.run(move |transaction, cache| {
				Self::try_delete_namespace_sqlite_sync(transaction, cache, &namespace)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the namespace"))
	}

	fn try_delete_namespace_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<Option<()>, db::sqlite::Error>> {
		let id = match Self::try_get_namespace_sqlite_sync_retry(transaction, cache, namespace)? {
			ControlFlow::Break(id) => id,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let Some(id) = id else {
			return Ok(ControlFlow::Break(None));
		};
		let result = transaction.query_row(
			"select count(*) from namespaces where parent = ?1;",
			sqlite::params![id],
			|row| row.get::<_, i64>(0),
		);
		let children = crate::database::retry!(
			result.map_err(db::sqlite::Error::from),
			"failed to execute the statement"
		);
		let result = transaction.query_row(
			"select count(*) from tags where namespace = ?1;",
			sqlite::params![id],
			|row| row.get::<_, i64>(0),
		);
		let tags = crate::database::retry!(
			result.map_err(db::sqlite::Error::from),
			"failed to execute the statement"
		);
		if children > 0 || tags > 0 {
			return Err(tg::error!("namespace is not empty"));
		}
		let result = transaction
			.execute("delete from namespaces where id = ?1;", sqlite::params![id])
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(Some(())))
	}
}
