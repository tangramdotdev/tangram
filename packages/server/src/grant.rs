use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_get_namespace_id_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<i64>> {
		if namespace.is_root() {
			return Ok(Some(0));
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select id
				from namespaces
				where name = {p}1;
			"
		);
		let params = db::params![namespace.to_string()];
		transaction
			.query_optional_value_into::<i64>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	pub(crate) async fn get_namespace_ancestor_ids_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<i64>> {
		let mut ids = vec![0];
		let p = transaction.p();
		let mut name = String::new();
		for component in namespace.components() {
			if !name.is_empty() {
				name.push('/');
			}
			name.push_str(component);
			let statement = formatdoc!(
				"
					select id
					from namespaces
					where name = {p}1;
				"
			);
			let params = db::params![name.clone()];
			if let Some(id) = transaction
				.query_optional_value_into::<i64>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			{
				ids.push(id);
			}
		}
		Ok(ids)
	}

	pub(crate) async fn get_or_create_namespace_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<i64> {
		if namespace.is_root() {
			return Ok(0);
		}

		let p = transaction.p();
		let mut parent = 0;
		let mut name = String::new();
		for component in namespace.components() {
			if !name.is_empty() {
				name.push('/');
			}
			name.push_str(component);

			let statement = formatdoc!(
				"
					select id
					from namespaces
					where name = {p}1;
				"
			);
			let params = db::params![name.clone()];
			if let Some(id) = transaction
				.query_optional_value_into::<i64>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			{
				parent = id;
				continue;
			}

			let statement = formatdoc!(
				"
					insert into namespaces (parent, component, name)
					values ({p}1, {p}2, {p}3)
					on conflict (name) do nothing;
				"
			);
			let params = db::params![parent, component, name.clone()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

			let statement = formatdoc!(
				"
					select id
					from namespaces
					where name = {p}1;
				"
			);
			let params = db::params![name.clone()];
			parent = transaction
				.query_one_value_into::<i64>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}

		Ok(parent)
	}

	pub(crate) async fn grant_namespace_to_user_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		user: &tg::user::Id,
		permission: tg::Permission,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::Grant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let permission = permission.to_string();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			"
				insert into namespace_grants (namespace, \"user\", permission, created_at, created_by)
				select {p}1, {p}2, {p}3, {p}4, {p}5
				where not exists (
					select 1
					from namespace_grants
					where namespace = {p}1 and \"user\" = {p}2 and permission = {p}3
				);
			"
		);
		let params = db::params![
			namespace_id,
			user.to_string(),
			permission.clone(),
			created_at,
			created_by,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Self::get_user_namespace_grant_with_transaction(
			transaction,
			namespace,
			namespace_id,
			user,
			&permission,
		)
		.await
	}

	pub(crate) async fn grant_namespace_to_group_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		group: &tg::group::Id,
		permission: tg::Permission,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::Grant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let permission = permission.to_string();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			"
				insert into namespace_grants (namespace, \"group\", permission, created_at, created_by)
				select {p}1, {p}2, {p}3, {p}4, {p}5
				where not exists (
					select 1
					from namespace_grants
					where namespace = {p}1 and \"group\" = {p}2 and permission = {p}3
				);
			"
		);
		let params = db::params![
			namespace_id,
			group.to_string(),
			permission.clone(),
			created_at,
			created_by,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Self::get_group_namespace_grant_with_transaction(
			transaction,
			namespace,
			namespace_id,
			group,
			&permission,
		)
		.await
	}

	pub(crate) async fn list_namespace_grants_for_user_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
	) -> tg::Result<Vec<tg::Grant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			namespace: tg::Namespace,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			user: Option<tg::user::Id>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			group: Option<tg::group::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.\"user\" as \"user\",
					namespace_grants.\"group\" as \"group\",
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.\"user\" = {p}1
				order by namespace_grants.namespace, namespace_grants.permission;
			"
		);
		let params = db::params![user.to_string()];
		let rows = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::Grant {
				namespace: row.namespace,
				user: row.user,
				group: row.group,
				permission: row.permission,
				created_at: row.created_at,
				created_by: row.created_by,
			})
			.collect())
	}

	pub(crate) async fn list_namespace_grants_for_group_with_transaction(
		transaction: &Transaction<'_>,
		group: &tg::group::Id,
	) -> tg::Result<Vec<tg::Grant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			namespace: tg::Namespace,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			user: Option<tg::user::Id>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			group: Option<tg::group::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.\"user\" as \"user\",
					namespace_grants.\"group\" as \"group\",
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.\"group\" = {p}1
				order by namespace_grants.namespace, namespace_grants.permission;
			"
		);
		let params = db::params![group.to_string()];
		let rows = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::Grant {
				namespace: row.namespace,
				user: row.user,
				group: row.group,
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
			#[tangram_database(as = "Option<db::value::FromStr>")]
			user: Option<tg::user::Id>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			group: Option<tg::group::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.\"user\" as \"user\",
					namespace_grants.\"group\" as \"group\",
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.namespace = {p}1
				order by namespace_grants.\"user\", namespace_grants.\"group\", namespace_grants.permission;
			"
		);
		let params = db::params![namespace_id];
		let rows = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::Grant {
				namespace: row.namespace,
				user: row.user,
				group: row.group,
				permission: row.permission,
				created_at: row.created_at,
				created_by: row.created_by,
			})
			.collect())
	}

	pub(crate) async fn list_effective_namespace_permissions_for_user_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<tg::Permission>> {
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
				"
					select namespace_grants.permission
					from namespace_grants
					where namespace_grants.namespace = {p}1
						and (
							namespace_grants.\"user\" = {p}2
							or exists (
								select 1
								from group_members
								where group_members.\"group\" = namespace_grants.\"group\"
									and group_members.\"user\" = {p}2
							)
						);
				"
			);
			let params = db::params![namespace_id, user.to_string()];
			let rows = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			for row in rows {
				for permission in [tg::Permission::Read, tg::Permission::Write] {
					if row.permission.implies(permission) {
						permissions.insert(permission);
					}
				}
			}
		}
		Ok(permissions.into_iter().collect())
	}

	async fn get_user_namespace_grant_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		user: &tg::user::Id,
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
			"
				select created_at, created_by
				from namespace_grants
				where namespace = {p}1 and \"user\" = {p}2 and permission = {p}3;
			"
		);
		let params = db::params![namespace_id, user.to_string(), permission];
		let row = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::Grant {
			namespace: namespace.clone(),
			user: Some(user.clone()),
			group: None,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	async fn get_group_namespace_grant_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		group: &tg::group::Id,
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
			"
				select created_at, created_by
				from namespace_grants
				where namespace = {p}1 and \"group\" = {p}2 and permission = {p}3;
			"
		);
		let params = db::params![namespace_id, group.to_string(), permission];
		let row = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::Grant {
			namespace: namespace.clone(),
			user: None,
			group: Some(group.clone()),
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}
}
