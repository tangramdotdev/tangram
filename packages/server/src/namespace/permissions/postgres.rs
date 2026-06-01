use {
	crate::Session,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn increment_namespace_visibility_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		let principal = principal.to_string();
		let namespace_ids = match namespace_visibility_ids_postgres(transaction, namespace).await? {
			ControlFlow::Break(namespace_ids) => namespace_ids,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for namespace_id in namespace_ids {
			let statement = indoc!(
				r"
					insert into namespace_visibility (namespace, principal, count)
					values ($1, $2, 1)
					on conflict (namespace, principal)
					do update set count = namespace_visibility.count + 1;
				"
			);
			let result = transaction
				.inner()
				.execute(statement, &[&namespace_id, &principal])
				.await
				.map_err(db::postgres::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn increment_namespace_visibility_for_user_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
		user: &tg::user::Id,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		Self::increment_namespace_visibility_postgres(
			transaction,
			namespace,
			&tg::Principal::User(user.clone()),
		)
		.await
	}

	pub(crate) async fn increment_namespace_visibility_for_all_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		Self::increment_namespace_visibility_postgres(transaction, namespace, &tg::Principal::All)
			.await
	}

	pub(crate) async fn decrement_namespace_visibility_for_grant_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
		user: Option<&str>,
		group: Option<&str>,
		all: bool,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		let namespace_ids = match namespace_visibility_ids_postgres(transaction, namespace).await? {
			ControlFlow::Break(namespace_ids) => namespace_ids,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for namespace_id in namespace_ids {
			let principal = if let Some(user) = user {
				user
			} else if let Some(group) = group {
				group
			} else if all {
				"all"
			} else {
				continue;
			};
			match decrement_namespace_visibility_for_principal_postgres(
				transaction,
				namespace_id,
				principal,
			)
			.await?
			{
				ControlFlow::Break(()) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn list_effective_namespace_permissions_for_user_postgres(
		transaction: &db::postgres::Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<tg::Permission>> {
		let names = ancestor_names(namespace);
		let user = user.to_string();
		let statement = indoc!(
			r#"
				with ancestor_ids(id) as (
					values (0::bigint)
					union
					select namespaces.id
					from namespaces
					where namespaces.name = any($1)
				),
				matching_principals(principal) as (
					values ($2::text), ('all'::text)
					union all
					select group_members."group"
					from group_members
					where group_members."user" = $2
				)
				select namespace_grants.permission
				from namespace_grants
				where namespace_grants.namespace in (select id from ancestor_ids)
					and exists (
						select 1
						from matching_principals
						where namespace_grants.principal = matching_principals.principal
					);
			"#
		);
		let rows = transaction
			.inner()
			.query(statement, &[&names, &user])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		permissions_from_rows(rows)
	}

	pub(crate) async fn list_effective_tag_permissions_for_user_postgres(
		transaction: &db::postgres::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
	) -> tg::Result<Vec<tg::Permission>> {
		let names = ancestor_names(&tag.namespace);
		let namespace = tag.namespace.to_string();
		let name = tag.name.to_string();
		let user = user.to_string();
		let statement = indoc!(
			r#"
				with ancestor_ids(id) as (
					values (0::bigint)
					union
					select namespaces.id
					from namespaces
					where namespaces.name = any($1)
				),
				tag_namespace(id) as (
					select 0::bigint
					where $2 = ''
					union
					select namespaces.id
					from namespaces
					where namespaces.name = $2
				),
				matching_principals(principal) as (
					values ($4::text), ('all'::text)
					union all
					select group_members."group"
					from group_members
					where group_members."user" = $4
				)
				select namespace_grants.permission
				from namespace_grants
				where namespace_grants.namespace in (select id from ancestor_ids)
					and exists (
						select 1
						from matching_principals
						where namespace_grants.principal = matching_principals.principal
					)
				union all
				select tag_grants.permission
				from tag_grants
				where tag_grants.namespace in (select id from tag_namespace)
					and tag_grants.name = $3
					and exists (
						select 1
						from matching_principals
						where tag_grants.principal = matching_principals.principal
					);
			"#
		);
		let rows = transaction
			.inner()
			.query(statement, &[&names, &namespace, &name, &user])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		permissions_from_rows(rows)
	}

	pub(crate) async fn namespace_has_all_read_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		let names = ancestor_names(namespace);
		let statement = indoc!(
			r"
				with ancestor_ids(id) as (
					values (0::bigint)
					union
					select namespaces.id
					from namespaces
					where namespaces.name = any($1)
				)
				select exists (
					select 1
					from namespace_grants
					where namespace_grants.namespace in (select id from ancestor_ids)
						and namespace_grants.principal = 'all'
						and namespace_grants.permission = 'read'
				);
			"
		);
		let rows = transaction
			.inner()
			.query(statement, &[&names])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		rows.first()
			.ok_or_else(|| tg::error!("expected a row"))?
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the exists column"))
	}

	pub(crate) async fn tag_has_all_read_postgres(
		transaction: &db::postgres::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let names = ancestor_names(&tag.namespace);
		let namespace = tag.namespace.to_string();
		let name = tag.name.to_string();
		let statement = indoc!(
			r"
				with ancestor_ids(id) as (
					values (0::bigint)
					union
					select namespaces.id
					from namespaces
					where namespaces.name = any($1)
				),
				tag_namespace(id) as (
					select 0::bigint
					where $2 = ''
					union
					select namespaces.id
					from namespaces
					where namespaces.name = $2
				)
				select exists (
					select 1
					from namespace_grants
					where namespace_grants.namespace in (select id from ancestor_ids)
						and namespace_grants.principal = 'all'
						and namespace_grants.permission = 'read'
				) or exists (
					select 1
					from tag_grants
					where tag_grants.namespace in (select id from tag_namespace)
						and tag_grants.name = $3
						and tag_grants.principal = 'all'
						and tag_grants.permission = 'read'
				);
			"
		);
		let rows = transaction
			.inner()
			.query(statement, &[&names, &namespace, &name])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		rows.first()
			.ok_or_else(|| tg::error!("expected a row"))?
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the exists column"))
	}
}

fn ancestor_names(namespace: &tg::Namespace) -> Vec<String> {
	let mut names = Vec::new();
	let mut name = String::new();
	for component in namespace.components() {
		if !name.is_empty() {
			name.push('/');
		}
		name.push_str(component);
		names.push(name.clone());
	}
	names
}

fn parse_permission(permission: &str) -> tg::Result<tg::Permission> {
	permission
		.parse()
		.map_err(|error| tg::error!(!error, "invalid permission"))
}

fn permissions_from_rows(rows: Vec<tokio_postgres::Row>) -> tg::Result<Vec<tg::Permission>> {
	let mut permissions = std::collections::BTreeSet::new();
	for row in rows {
		let row_permission = parse_permission(
			&row.try_get::<_, String>(0)
				.map_err(|error| tg::error!(!error, "failed to get the permission column"))?,
		)?;
		for permission in [
			tg::Permission::Admin,
			tg::Permission::Read,
			tg::Permission::Write,
		] {
			if row_permission.implies(permission) {
				permissions.insert(permission);
			}
		}
	}
	Ok(permissions.into_iter().collect())
}

async fn namespace_visibility_ids_postgres(
	transaction: &db::postgres::Transaction<'_>,
	namespace: &tg::Namespace,
) -> tg::Result<ControlFlow<Vec<i64>, db::postgres::Error>> {
	let names = ancestor_names(namespace);
	if names.is_empty() {
		return Ok(ControlFlow::Break(Vec::new()));
	}
	let statement = indoc!(
		"
			select id
			from namespaces
			where name = any($1);
		"
	);
	let result = transaction
		.inner()
		.query(statement, &[&names])
		.await
		.map_err(db::postgres::Error::from);
	let rows = crate::database::retry!(result, "failed to execute the statement");
	let ids = rows
		.into_iter()
		.map(|row| {
			row.try_get(0)
				.map_err(|error| tg::error!(!error, "failed to get the id column"))
		})
		.collect::<tg::Result<Vec<_>>>()?;
	Ok(ControlFlow::Break(ids))
}

async fn decrement_namespace_visibility_for_principal_postgres(
	transaction: &db::postgres::Transaction<'_>,
	namespace_id: i64,
	principal: &str,
) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
	let statement = indoc!(
		r"
			delete from namespace_visibility
			where namespace = $1 and principal = $2 and count = 1;
		"
	);
	let result = transaction
		.inner()
		.execute(statement, &[&namespace_id, &principal])
		.await
		.map_err(db::postgres::Error::from);
	let deleted = crate::database::retry!(result, "failed to execute the statement");
	if deleted == 0 {
		let statement = indoc!(
			r"
				update namespace_visibility
				set count = count - 1
				where namespace = $1 and principal = $2 and count > 1;
			"
		);
		let result = transaction
			.inner()
			.execute(statement, &[&namespace_id, &principal])
			.await
			.map_err(db::postgres::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
	}
	Ok(ControlFlow::Break(()))
}
