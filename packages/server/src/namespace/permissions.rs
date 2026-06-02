use {
	crate::{Session, context::Authentication, database::Transaction},
	indoc::formatdoc,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

impl Session {
	pub(crate) async fn list_effective_namespace_permissions_for_user_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<tg::Permission>> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::list_effective_namespace_permissions_for_user_postgres(
				transaction,
				user,
				namespace,
			)
			.await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::list_effective_namespace_permissions_for_user_sqlite(
				transaction,
				user,
				namespace,
			)
			.await;
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
		}

		let mut permissions = BTreeSet::new();
		let p = transaction.p();
		for namespace_id in
			Self::get_namespace_ancestor_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					select namespace_grants.permission
					from namespace_grants
					where namespace_grants.namespace = {p}1
						and (
							namespace_grants.principal = {p}2
							or namespace_grants.principal = 'all'
							or exists (
								select 1
								from group_members
								where group_members."group" = namespace_grants.principal
									and group_members."user" = {p}2
							)
						);
				"#
			);
			let params = db::params![namespace_id, user.to_string()];
			let rows = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			for row in rows {
				for permission in [
					tg::Permission::Admin,
					tg::Permission::Read,
					tg::Permission::Write,
				] {
					if row.permission.implies(permission) {
						permissions.insert(permission);
					}
				}
			}
		}
		Ok(permissions.into_iter().collect())
	}

	pub(crate) async fn user_has_namespace_permission_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		let permissions = Self::list_effective_namespace_permissions_for_user_with_transaction(
			transaction,
			user,
			namespace,
		)
		.await?;
		Ok(permissions.contains(&permission))
	}

	pub(crate) async fn namespace_has_all_read_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::namespace_has_all_read_postgres(transaction, namespace).await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::namespace_has_all_read_sqlite(transaction, namespace).await;
		}

		let p = transaction.p();
		for namespace_id in
			Self::get_namespace_ancestor_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r"
				select 1
				from namespace_grants
				where namespace = {p}1 and principal = 'all' and permission = 'read';
			"
			);
			if transaction
				.query_optional(statement.into(), db::params![namespace_id])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				.is_some()
			{
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub(crate) async fn authorize_namespace(
		&self,
		namespace: &tg::Namespace,
		permission: tg::Permission,
	) -> tg::Result<()> {
		if permission != tg::Permission::Read {
			let user = match &self.context.authentication {
				Some(Authentication::Root) => return Ok(()),
				Some(Authentication::User(user)) => user,
				_ => return Err(tg::error!("unauthorized")),
			};
			let mut connection = self
				.server
				.database
				.connection()
				.await
				.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
			let transaction = connection
				.transaction()
				.await
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			if Self::user_has_namespace_permission_with_transaction(
				&transaction,
				&user.id,
				namespace,
				permission,
			)
			.await?
			{
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		}

		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		match &self.context.authentication {
			Some(Authentication::Root) => Ok(()),
			Some(Authentication::User(user)) => {
				if Self::user_has_namespace_permission_with_transaction(
					&transaction,
					&user.id,
					namespace,
					tg::Permission::Read,
				)
				.await?
				{
					Ok(())
				} else {
					Err(tg::error!("unauthorized"))
				}
			},
			None
			| Some(
				Authentication::Process(_) | Authentication::Runner | Authentication::Sandbox(_),
			) => {
				if Self::namespace_has_all_read_with_transaction(&transaction, namespace).await? {
					Ok(())
				} else {
					Err(tg::error!("unauthorized"))
				}
			},
		}
	}
}
