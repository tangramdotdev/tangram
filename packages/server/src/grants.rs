use {
	crate::{Session, context::Authentication, database::Transaction},
	indoc::formatdoc,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[derive(Clone)]
enum NamespaceReadSubject {
	All,
	Public,
	User(tg::user::Id),
}

#[derive(db::row::Deserialize)]
struct PermissionRow {
	#[tangram_database(as = "db::value::FromStr")]
	permission: tg::Permission,
}

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
			r"
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

	pub(crate) async fn namespace_path_has_tag_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		let components = namespace.components().collect::<Vec<_>>();
		let Some((name, namespace_components)) = components.split_last() else {
			return Ok(false);
		};
		let parent = tg::Namespace::with_components(
			namespace_components
				.iter()
				.map(|component| (*component).to_owned()),
		);
		let Some(parent_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &parent).await?
		else {
			return Ok(false);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select 1
				from tags
				where namespace = {p}1 and name = {p}2;
			"
		);
		transaction
			.query_optional(statement.into(), db::params![parent_id, *name])
			.await
			.map(|row| row.is_some())
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	pub(crate) async fn try_get_tag_namespace_id_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<Option<i64>> {
		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace).await?
		else {
			return Ok(None);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select 1
				from tags
				where namespace = {p}1 and name = {p}2;
			"
		);
		transaction
			.query_optional(
				statement.into(),
				db::params![namespace_id, tag.name.to_string()],
			)
			.await
			.map(|row| row.map(|_| namespace_id))
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	pub(crate) async fn get_namespace_ancestor_ids_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<i64>> {
		let mut ids = vec![0];
		let mut names = Vec::new();
		let mut name = String::new();
		for component in namespace.components() {
			if !name.is_empty() {
				name.push('/');
			}
			name.push_str(component);
			names.push(name.clone());
		}
		if names.is_empty() {
			return Ok(ids);
		}
		let p = transaction.p();
		let placeholders = (1..=names.len())
			.map(|i| format!("{p}{i}"))
			.collect::<Vec<_>>()
			.join(", ");
		let statement = formatdoc!(
			r"
				select id
				from namespaces
				where name in ({placeholders});
			"
		);
		let params = names.into_iter().map(db::Value::Text).collect();
		ids.extend(
			transaction
				.query_all_value_into::<i64>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?,
		);
		Ok(ids)
	}

	pub(crate) async fn get_or_create_namespace_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<i64> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::get_or_create_namespace_postgres(transaction, namespace).await;
		}

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
				r"
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
				r"
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
				r"
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

	pub(crate) async fn create_namespace_grant_for_user_with_transaction(
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
			r#"
				insert into namespace_grants (namespace, "user", permission, created_at, created_by)
				select {p}1, {p}2, {p}3, {p}4, {p}5
				where not exists (
					select 1
					from namespace_grants
					where namespace = {p}1 and "user" = {p}2 and permission = {p}3
				);
			"#
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

		Self::get_namespace_grant_for_user_with_transaction(
			transaction,
			namespace,
			namespace_id,
			user,
			&permission,
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
	) -> tg::Result<tg::Grant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let permission = permission.to_string();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			r#"
				insert into namespace_grants (namespace, "group", permission, created_at, created_by)
				select {p}1, {p}2, {p}3, {p}4, {p}5
				where not exists (
					select 1
					from namespace_grants
					where namespace = {p}1 and "group" = {p}2 and permission = {p}3
				);
			"#
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

		Self::get_namespace_grant_for_group_with_transaction(
			transaction,
			namespace,
			namespace_id,
			group,
			&permission,
		)
		.await
	}

	pub(crate) async fn create_namespace_grant_for_public_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::Grant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			r#"
				insert into namespace_grants (namespace, "public", permission, created_at, created_by)
				select {p}1, true, 'read', {p}2, {p}3
				where not exists (
					select 1
					from namespace_grants
					where namespace = {p}1 and "public" and permission = 'read'
				);
			"#
		);
		let params = db::params![namespace_id, created_at, created_by];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Self::get_namespace_grant_for_public_with_transaction(transaction, namespace, namespace_id)
			.await
	}

	pub(crate) async fn delete_namespace_grant_for_user_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		user: &tg::user::Id,
		permission: tg::Permission,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from namespace_grants
				where namespace = {p}1 and "user" = {p}2 and permission = {p}3;
			"#
		);
		let params = db::params![namespace_id, user.to_string(), permission.to_string()];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn delete_namespace_grant_for_group_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		group: &tg::group::Id,
		permission: tg::Permission,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from namespace_grants
				where namespace = {p}1 and "group" = {p}2 and permission = {p}3;
			"#
		);
		let params = db::params![namespace_id, group.to_string(), permission.to_string()];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn delete_namespace_grant_for_public_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from namespace_grants
				where namespace = {p}1 and "public" and permission = 'read';
			"#
		);
		let n = transaction
			.execute(statement.into(), db::params![namespace_id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
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
			public: bool,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants."user" as "user",
					namespace_grants."group" as "group",
					namespace_grants."public" as public,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants."user" = {p}1
				order by namespace_grants.namespace, namespace_grants.permission;
			"#
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
				public: row.public,
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
			public: bool,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants."user" as "user",
					namespace_grants."group" as "group",
					namespace_grants."public" as public,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants."group" = {p}1
				order by namespace_grants.namespace, namespace_grants.permission;
			"#
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
				public: row.public,
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
			public: bool,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants."user" as "user",
					namespace_grants."group" as "group",
					namespace_grants."public" as public,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.namespace = {p}1
				order by namespace_grants."user", namespace_grants."group", namespace_grants.permission;
			"#
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
				public: row.public,
				permission: row.permission,
				created_at: row.created_at,
				created_by: row.created_by,
			})
			.collect())
	}

	pub(crate) async fn create_tag_grant_for_user_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		namespace_id: i64,
		user: &tg::user::Id,
		permission: tg::Permission,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::TagGrant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let permission = permission.to_string();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			r#"
				insert into tag_grants (namespace, name, "user", permission, created_at, created_by)
				select {p}1, {p}2, {p}3, {p}4, {p}5, {p}6
				where not exists (
					select 1
					from tag_grants
					where namespace = {p}1 and name = {p}2 and "user" = {p}3 and permission = {p}4
				);
			"#
		);
		let params = db::params![
			namespace_id,
			tag.name.to_string(),
			user.to_string(),
			permission.clone(),
			created_at,
			created_by,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Self::get_tag_grant_for_user_with_transaction(
			transaction,
			tag,
			namespace_id,
			user,
			&permission,
		)
		.await
	}

	pub(crate) async fn create_tag_grant_for_group_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		namespace_id: i64,
		group: &tg::group::Id,
		permission: tg::Permission,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::TagGrant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let permission = permission.to_string();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			r#"
				insert into tag_grants (namespace, name, "group", permission, created_at, created_by)
				select {p}1, {p}2, {p}3, {p}4, {p}5, {p}6
				where not exists (
					select 1
					from tag_grants
					where namespace = {p}1 and name = {p}2 and "group" = {p}3 and permission = {p}4
				);
			"#
		);
		let params = db::params![
			namespace_id,
			tag.name.to_string(),
			group.to_string(),
			permission.clone(),
			created_at,
			created_by,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Self::get_tag_grant_for_group_with_transaction(
			transaction,
			tag,
			namespace_id,
			group,
			&permission,
		)
		.await
	}

	pub(crate) async fn create_tag_grant_for_public_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		namespace_id: i64,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::TagGrant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			r#"
				insert into tag_grants (namespace, name, "public", permission, created_at, created_by)
				select {p}1, {p}2, true, 'read', {p}3, {p}4
				where not exists (
					select 1
					from tag_grants
					where namespace = {p}1 and name = {p}2 and "public" and permission = 'read'
				);
			"#
		);
		let params = db::params![namespace_id, tag.name.to_string(), created_at, created_by,];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Self::get_tag_grant_for_public_with_transaction(transaction, tag, namespace_id).await
	}

	pub(crate) async fn delete_tag_grant_for_user_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		name: &tg::tag::Name,
		user: &tg::user::Id,
		permission: tg::Permission,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from tag_grants
				where namespace = {p}1 and name = {p}2 and "user" = {p}3 and permission = {p}4;
			"#
		);
		let params = db::params![
			namespace_id,
			name.to_string(),
			user.to_string(),
			permission.to_string(),
		];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn delete_tag_grant_for_group_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		name: &tg::tag::Name,
		group: &tg::group::Id,
		permission: tg::Permission,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from tag_grants
				where namespace = {p}1 and name = {p}2 and "group" = {p}3 and permission = {p}4;
			"#
		);
		let params = db::params![
			namespace_id,
			name.to_string(),
			group.to_string(),
			permission.to_string(),
		];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn delete_tag_grant_for_public_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		name: &tg::tag::Name,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from tag_grants
				where namespace = {p}1 and name = {p}2 and "public" and permission = 'read';
			"#
		);
		let n = transaction
			.execute(
				statement.into(),
				db::params![namespace_id, name.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn list_tag_grants_for_tag_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		namespace_id: i64,
	) -> tg::Result<Vec<tg::TagGrant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			user: Option<tg::user::Id>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			group: Option<tg::group::Id>,
			public: bool,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::Permission,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select
					tag_grants."user" as "user",
					tag_grants."group" as "group",
					tag_grants."public" as public,
					tag_grants.permission,
					tag_grants.created_at,
					tag_grants.created_by
				from tag_grants
				where tag_grants.namespace = {p}1 and tag_grants.name = {p}2
				order by tag_grants."user", tag_grants."group", tag_grants.permission;
			"#
		);
		let params = db::params![namespace_id, tag.name.to_string()];
		let rows = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::TagGrant {
				tag: tag.clone(),
				user: row.user,
				group: row.group,
				public: row.public,
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
							namespace_grants."user" = {p}2
							or namespace_grants."public"
							or exists (
								select 1
								from group_members
								where group_members."group" = namespace_grants."group"
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
					select tag_grants.permission
					from tag_grants
					where tag_grants.namespace = {p}1
						and tag_grants.name = {p}2
						and (
							tag_grants."user" = {p}3
							or tag_grants."public"
							or exists (
								select 1
								from group_members
								where group_members."group" = tag_grants."group"
									and group_members."user" = {p}3
							)
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

	pub(crate) async fn user_has_exact_tag_permission_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::user_has_exact_tag_permission_postgres(
				transaction,
				user,
				tag,
				permission,
			)
			.await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::user_has_exact_tag_permission_sqlite(transaction, user, tag, permission)
				.await;
		}

		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace).await?
		else {
			return Ok(false);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select tag_grants.permission
				from tag_grants
				where tag_grants.namespace = {p}1
					and tag_grants.name = {p}2
					and (
						tag_grants."user" = {p}3
						or tag_grants."public"
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants."group"
								and group_members."user" = {p}3
						)
					);
			"#
		);
		let params = db::params![namespace_id, tag.name.to_string(), user.to_string()];
		let rows = transaction
			.query_all_into::<PermissionRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.any(|row| row.permission.implies(permission)))
	}

	pub(crate) async fn namespace_has_public_read_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::namespace_has_public_read_postgres(transaction, namespace).await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::namespace_has_public_read_sqlite(transaction, namespace).await;
		}

		let p = transaction.p();
		for namespace_id in
			Self::get_namespace_ancestor_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					select 1
					from namespace_grants
					where namespace = {p}1 and "public" and permission = 'read';
				"#
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

	pub(crate) async fn tag_has_public_read_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::tag_has_public_read_postgres(transaction, tag).await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::tag_has_public_read_sqlite(transaction, tag).await;
		}

		if Self::namespace_has_public_read_with_transaction(transaction, &tag.namespace).await? {
			return Ok(true);
		}
		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace).await?
		else {
			return Ok(false);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select 1
				from tag_grants
				where namespace = {p}1 and name = {p}2 and "public" and permission = 'read';
			"#
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

	pub(crate) async fn tag_has_exact_public_read_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::tag_has_exact_public_read_postgres(transaction, tag).await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = transaction {
			return Self::tag_has_exact_public_read_sqlite(transaction, tag).await;
		}

		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace).await?
		else {
			return Ok(false);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select 1
				from tag_grants
				where namespace = {p}1 and name = {p}2 and "public" and permission = 'read';
			"#
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

	fn namespace_read_subject(&self) -> NamespaceReadSubject {
		match &self.context.authentication {
			Some(Authentication::Root) => NamespaceReadSubject::All,
			Some(Authentication::User(user)) => NamespaceReadSubject::User(user.id.clone()),
			None | Some(Authentication::Process(_) | Authentication::Runner) => {
				NamespaceReadSubject::Public
			},
			Some(Authentication::Sandbox(_)) => NamespaceReadSubject::Public,
		}
	}

	pub(crate) fn authorize_list(&self) {
		let _ = self.namespace_read_subject();
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

		let subject = self.namespace_read_subject();
		if matches!(subject, NamespaceReadSubject::All) {
			return Ok(());
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
		match subject {
			NamespaceReadSubject::All => Ok(()),
			NamespaceReadSubject::Public => {
				if Self::namespace_has_public_read_with_transaction(&transaction, namespace).await?
				{
					Ok(())
				} else {
					Err(tg::error!("unauthorized"))
				}
			},
			NamespaceReadSubject::User(user) => {
				if Self::user_has_namespace_permission_with_transaction(
					&transaction,
					&user,
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
		}
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

		let subject = self.namespace_read_subject();
		if matches!(subject, NamespaceReadSubject::All) {
			return Ok(());
		}
		match subject {
			NamespaceReadSubject::All => Ok(()),
			NamespaceReadSubject::Public => {
				if Self::tag_has_public_read_with_transaction(transaction, tag).await? {
					Ok(())
				} else {
					Err(tg::error!("unauthorized"))
				}
			},
			NamespaceReadSubject::User(user) => {
				if Self::user_has_tag_permission_with_transaction(
					transaction,
					&user,
					tag,
					tg::Permission::Read,
				)
				.await?
				{
					Ok(())
				} else {
					Err(tg::error!("unauthorized"))
				}
			},
		}
	}

	pub(crate) async fn filter_list_entries_by_read_permission(
		&self,
		data: Vec<tg::list::Entry>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let subject = self.namespace_read_subject();
		if matches!(subject, NamespaceReadSubject::All) {
			return Ok(data);
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
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = &transaction {
			return Self::filter_list_entries_by_read_permission_postgres(
				transaction,
				&subject,
				data,
			)
			.await;
		}
		#[cfg(feature = "sqlite")]
		if let Transaction::Sqlite(transaction) = &transaction {
			return Self::filter_list_entries_by_read_permission_sqlite(
				transaction,
				&subject,
				data,
			)
			.await;
		}

		let mut filtered = Vec::new();
		let mut readable_by_namespace = BTreeMap::new();
		let mut readable_by_tag = BTreeMap::new();
		for entry in data {
			let readable = match &entry {
				tg::list::Entry::Namespace { namespace, .. } => {
					if let Some(readable) = readable_by_namespace.get(namespace) {
						*readable
					} else {
						let readable = match &subject {
							NamespaceReadSubject::All => true,
							NamespaceReadSubject::Public => {
								Self::namespace_has_public_read_with_transaction(
									&transaction,
									namespace,
								)
								.await?
							},
							NamespaceReadSubject::User(user) => {
								Self::user_has_namespace_permission_with_transaction(
									&transaction,
									user,
									namespace,
									tg::Permission::Read,
								)
								.await?
							},
						};
						readable_by_namespace.insert(namespace.clone(), readable);
						readable
					}
				},
				tg::list::Entry::Tag { tag, .. } => {
					if let Some(readable) = readable_by_tag.get(tag) {
						*readable
					} else {
						let namespace_readable =
							if let Some(readable) = readable_by_namespace.get(&tag.namespace) {
								*readable
							} else {
								let readable = match &subject {
									NamespaceReadSubject::All => true,
									NamespaceReadSubject::Public => {
										Self::namespace_has_public_read_with_transaction(
											&transaction,
											&tag.namespace,
										)
										.await?
									},
									NamespaceReadSubject::User(user) => {
										Self::user_has_namespace_permission_with_transaction(
											&transaction,
											user,
											&tag.namespace,
											tg::Permission::Read,
										)
										.await?
									},
								};
								readable_by_namespace.insert(tag.namespace.clone(), readable);
								readable
							};
						let readable = namespace_readable
							|| match &subject {
								NamespaceReadSubject::All => true,
								NamespaceReadSubject::Public => {
									Self::tag_has_exact_public_read_with_transaction(
										&transaction,
										tag,
									)
									.await?
								},
								NamespaceReadSubject::User(user) => {
									Self::user_has_exact_tag_permission_with_transaction(
										&transaction,
										user,
										tag,
										tg::Permission::Read,
									)
									.await?
								},
							};
						readable_by_tag.insert(tag.clone(), readable);
						readable
					}
				},
			};
			if readable {
				filtered.push(entry);
			}
		}
		Ok(filtered)
	}

	async fn get_namespace_grant_for_user_with_transaction(
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
			r#"
				select created_at, created_by
				from namespace_grants
				where namespace = {p}1 and "user" = {p}2 and permission = {p}3;
			"#
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
			public: false,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	async fn get_tag_grant_for_user_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		namespace_id: i64,
		user: &tg::user::Id,
		permission: &str,
	) -> tg::Result<tg::TagGrant> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at, created_by
				from tag_grants
				where namespace = {p}1 and name = {p}2 and "user" = {p}3 and permission = {p}4;
			"#
		);
		let params = db::params![
			namespace_id,
			tag.name.to_string(),
			user.to_string(),
			permission,
		];
		let row = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::TagGrant {
			tag: tag.clone(),
			user: Some(user.clone()),
			group: None,
			public: false,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	async fn get_namespace_grant_for_group_with_transaction(
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
			r#"
				select created_at, created_by
				from namespace_grants
				where namespace = {p}1 and "group" = {p}2 and permission = {p}3;
			"#
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
			public: false,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	async fn get_tag_grant_for_group_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		namespace_id: i64,
		group: &tg::group::Id,
		permission: &str,
	) -> tg::Result<tg::TagGrant> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at, created_by
				from tag_grants
				where namespace = {p}1 and name = {p}2 and "group" = {p}3 and permission = {p}4;
			"#
		);
		let params = db::params![
			namespace_id,
			tag.name.to_string(),
			group.to_string(),
			permission,
		];
		let row = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::TagGrant {
			tag: tag.clone(),
			user: None,
			group: Some(group.clone()),
			public: false,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	async fn get_namespace_grant_for_public_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
	) -> tg::Result<tg::Grant> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at, created_by
				from namespace_grants
				where namespace = {p}1 and "public" and permission = 'read';
			"#
		);
		let row = transaction
			.query_one_into::<Row>(statement.into(), db::params![namespace_id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::Grant {
			namespace: namespace.clone(),
			user: None,
			group: None,
			public: true,
			permission: tg::Permission::Read,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	async fn get_tag_grant_for_public_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		namespace_id: i64,
	) -> tg::Result<tg::TagGrant> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at, created_by
				from tag_grants
				where namespace = {p}1 and name = {p}2 and "public" and permission = 'read';
			"#
		);
		let row = transaction
			.query_one_into::<Row>(
				statement.into(),
				db::params![namespace_id, tag.name.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(tg::TagGrant {
			tag: tag.clone(),
			user: None,
			group: None,
			public: true,
			permission: tg::Permission::Read,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}
}
