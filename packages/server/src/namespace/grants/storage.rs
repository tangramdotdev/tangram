use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn create_namespace_grant_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		principal: &tg::Principal,
		permission: tg::Permission,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<ControlFlow<tg::Grant, crate::database::Error>> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let implies_read = permission.implies(tg::Permission::Read);
		let permission = permission.to_string();
		let principal_string = principal.to_string();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			r"
				insert into namespace_grants (namespace, principal, permission, created_at, created_by)
				select {p}1, {p}2, {p}3, {p}4, {p}5
				where not exists (
					select 1
					from namespace_grants
					where namespace = {p}1 and principal = {p}2 and permission = {p}3
				);
			"
		);
		let params = db::params![
			namespace_id,
			principal_string.clone(),
			permission.clone(),
			created_at,
			created_by,
		];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n > 0 && implies_read {
			match Self::increment_namespace_visibility_with_transaction(
				transaction,
				namespace,
				principal,
			)
			.await?
			{
				ControlFlow::Break(()) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}

		let grant = Self::get_namespace_grant_with_transaction(
			transaction,
			namespace,
			namespace_id,
			principal,
			&permission,
		)
		.await?;
		Ok(ControlFlow::Break(grant))
	}

	pub(crate) async fn create_namespace_grant_for_user_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		user: &tg::user::Id,
		permission: tg::Permission,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<ControlFlow<tg::Grant, crate::database::Error>> {
		Self::create_namespace_grant_with_transaction(
			transaction,
			namespace,
			namespace_id,
			&tg::Principal::User(user.clone()),
			permission,
			created_by,
		)
		.await
	}

	pub(crate) async fn create_namespace_grant_for_group_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		group: &tg::group::Id,
		permission: tg::Permission,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<ControlFlow<tg::Grant, crate::database::Error>> {
		Self::create_namespace_grant_with_transaction(
			transaction,
			namespace,
			namespace_id,
			&tg::Principal::Group(group.clone()),
			permission,
			created_by,
		)
		.await
	}

	pub(crate) async fn create_namespace_grant_for_all_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<ControlFlow<tg::Grant, crate::database::Error>> {
		Self::create_namespace_grant_with_transaction(
			transaction,
			namespace,
			namespace_id,
			&tg::Principal::All,
			tg::Permission::Read,
			created_by,
		)
		.await
	}

	pub(crate) async fn delete_namespace_grant_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		principal: &tg::Principal,
		permission: tg::Permission,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				delete from namespace_grants
				where namespace = {p}1 and principal = {p}2 and permission = {p}3;
			"
		);
		let params = db::params![namespace_id, principal.to_string(), permission.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n > 0 && permission.implies(tg::Permission::Read) {
			let namespace = Self::namespace_for_id_with_transaction(transaction, namespace_id)
				.await?
				.ok_or_else(|| tg::error!("failed to find the namespace"))?;
			match Self::decrement_namespace_visibility_with_transaction(
				transaction,
				&namespace,
				principal,
			)
			.await?
			{
				ControlFlow::Break(()) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		Ok(ControlFlow::Break((n > 0).then_some(())))
	}

	pub(crate) async fn list_namespace_grants_for_user_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
	) -> tg::Result<Vec<tg::Grant>> {
		Self::list_namespace_grants_for_principal_with_transaction(
			transaction,
			&tg::Principal::User(user.clone()),
		)
		.await
	}

	pub(crate) async fn list_namespace_grants_for_group_with_transaction(
		transaction: &Transaction<'_>,
		group: &tg::group::Id,
	) -> tg::Result<Vec<tg::Grant>> {
		Self::list_namespace_grants_for_principal_with_transaction(
			transaction,
			&tg::Principal::Group(group.clone()),
		)
		.await
	}

	async fn list_namespace_grants_for_principal_with_transaction(
		transaction: &Transaction<'_>,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::Grant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			namespace: tg::Namespace,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.principal,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.principal = {p}1
				order by namespace_grants.namespace, namespace_grants.permission;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![principal.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::Grant {
				namespace: row.namespace,
				principal: row.principal,
				permission: row.permission,
				created_at: row.created_at,
				created_by: row.created_by,
			})
			.collect())
	}

	pub(crate) async fn list_namespace_grants_for_namespace_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
	) -> tg::Result<Vec<tg::Grant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			namespace: tg::Namespace,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.principal,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.namespace = {p}1
				order by namespace_grants.principal, namespace_grants.permission;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![namespace_id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::Grant {
				namespace: row.namespace,
				principal: row.principal,
				permission: row.permission,
				created_at: row.created_at,
				created_by: row.created_by,
			})
			.collect())
	}

	pub(crate) async fn get_namespace_grant_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		principal: &tg::Principal,
		permission: &str,
	) -> tg::Result<tg::Grant> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select created_at, created_by
				from namespace_grants
				where namespace = {p}1 and principal = {p}2 and permission = {p}3;
			"
		);
		let params = db::params![namespace_id, principal.to_string(), permission];
		let row = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::Grant {
			namespace: namespace.clone(),
			principal: principal.clone(),
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}
}
