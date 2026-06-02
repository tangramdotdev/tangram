use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_delete_namespace_turso(
		&self,
		database: &db::turso::Database,
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
					Self::try_delete_namespace_turso_with_transaction(transaction, &namespace).await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the namespace"))
	}

	async fn try_delete_namespace_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<Option<()>, db::turso::Error>> {
		let Some(id) =
			Self::try_get_namespace_turso_with_transaction(transaction, namespace).await?
		else {
			return Ok(ControlFlow::Break(None));
		};
		let result = transaction
			.query_one_value_into::<i64>(
				"select count(*) from namespaces where parent = ?1;".into(),
				db::params![id],
			)
			.await;
		let children = crate::database::retry!(result, "failed to execute the statement");
		let result = transaction
			.query_one_value_into::<i64>(
				"select count(*) from tags where namespace = ?1;".into(),
				db::params![id],
			)
			.await;
		let tags = crate::database::retry!(result, "failed to execute the statement");
		if children > 0 || tags > 0 {
			return Err(tg::error!("namespace is not empty"));
		}
		let result = transaction
			.execute(
				"delete from namespaces where id = ?1;".into(),
				db::params![id],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(Some(())))
	}
}
