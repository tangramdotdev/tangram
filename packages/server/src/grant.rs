use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
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
