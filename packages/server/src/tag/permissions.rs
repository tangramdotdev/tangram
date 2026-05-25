use {
	crate::{Session, context::Authentication, database::Transaction},
	indoc::formatdoc,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn list_effective_tag_permissions_for_user_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
	) -> tg::Result<Vec<tg::Permission>> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::list_effective_tag_permissions_for_user_postgres(transaction, user, tag)
				.await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::list_effective_tag_permissions_for_user_sqlite(transaction, user, tag)
				.await;
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
		}

		let mut permissions = Self::list_effective_namespace_permissions_for_user_with_transaction(
			transaction,
			user,
			&tag.namespace,
		)
		.await?
		.into_iter()
		.collect::<BTreeSet<_>>();
		if let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace).await?
		{
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					with matching_principals(principal) as (
						values ({p}3), ('all')
						union all
						select group_members."group"
						from group_members
						where group_members."user" = {p}3
					)
					select tag_grants.permission
					from tag_grants
					where tag_grants.namespace = {p}1
						and tag_grants.name = {p}2
						and exists (
							select 1
							from matching_principals
							where tag_grants.principal = matching_principals.principal
						);
				"#
			);
			let params = db::params![namespace_id, tag.name.to_string(), user.to_string()];
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

	pub(crate) async fn user_has_tag_permission_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::user_has_tag_permission_sqlite(transaction, user, tag, permission).await;
		}
		let permissions =
			Self::list_effective_tag_permissions_for_user_with_transaction(transaction, user, tag)
				.await?;
		Ok(permissions.contains(&permission))
	}

	pub(crate) async fn tag_has_all_read_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::tag_has_all_read_postgres(transaction, tag).await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::tag_has_all_read_sqlite(transaction, tag).await;
		}

		if Self::namespace_has_all_read_with_transaction(transaction, &tag.namespace).await? {
			return Ok(true);
		}
		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace).await?
		else {
			return Ok(false);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r"
			select 1
			from tag_grants
			where namespace = {p}1 and name = {p}2 and principal = 'all' and permission = 'read';
		"
		);
		transaction
			.query_optional(
				statement.into(),
				db::params![namespace_id, tag.name.to_string()],
			)
			.await
			.map(|row| row.is_some())
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	pub(crate) async fn authorize_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<()> {
		if permission != tg::Permission::Read {
			let user = match &self.context.authentication {
				Some(Authentication::Root) => return Ok(()),
				Some(Authentication::User(user)) => user,
				_ => return Err(tg::error!("unauthorized")),
			};
			if Self::user_has_tag_permission_with_transaction(
				transaction,
				&user.id,
				tag,
				permission,
			)
			.await?
			{
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		}

		if let Some(user) = Authentication::user_id(self.context.authentication.as_ref()) {
			if Self::user_has_tag_permission_with_transaction(
				transaction,
				user,
				tag,
				tg::Permission::Read,
			)
			.await?
			{
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		}
		if Authentication::uses_all_grants(self.context.authentication.as_ref()) {
			if Self::tag_has_all_read_with_transaction(transaction, tag).await? {
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		}
		match &self.context.authentication {
			Some(Authentication::Root) => Ok(()),
			_ => Err(tg::error!("unauthorized")),
		}
	}
}
