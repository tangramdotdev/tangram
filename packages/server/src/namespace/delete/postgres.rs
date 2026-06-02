use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
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
		let namespace = namespace.clone();
		database
			.run(|transaction| {
				let namespace = namespace.clone();
				async move {
					Self::try_delete_namespace_postgres_with_transaction(transaction, &namespace)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the namespace"))
	}

	async fn try_delete_namespace_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<Option<()>, db::postgres::Error>> {
		let Some(id) =
			Self::try_get_namespace_postgres_with_transaction(transaction, namespace).await?
		else {
			return Ok(ControlFlow::Break(None));
		};
		let result = transaction
			.inner()
			.query("select count(*) from namespaces where parent = $1;", &[&id])
			.await
			.map_err(db::postgres::Error::from);
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let children: i64 = rows
			.first()
			.ok_or_else(|| tg::error!("expected a row"))?
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the count column"))?;
		let result = transaction
			.inner()
			.query("select count(*) from tags where namespace = $1;", &[&id])
			.await
			.map_err(db::postgres::Error::from);
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let tags: i64 = rows
			.first()
			.ok_or_else(|| tg::error!("expected a row"))?
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the count column"))?;
		if children > 0 || tags > 0 {
			return Err(tg::error!("namespace is not empty"));
		}
		let result = transaction
			.inner()
			.execute("delete from namespaces where id = $1;", &[&id])
			.await
			.map_err(db::postgres::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(Some(())))
	}
}
