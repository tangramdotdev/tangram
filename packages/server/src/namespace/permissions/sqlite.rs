use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(super) async fn list_effective_namespace_permissions_for_user_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<tg::Permission>> {
		let namespace = namespace.clone();
		let user = user.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::list_effective_namespace_permissions_for_user_sqlite_sync(
					transaction,
					&user,
					&namespace,
				)
			})
			.await
	}

	pub(crate) async fn list_effective_tag_permissions_for_user_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
	) -> tg::Result<Vec<tg::Permission>> {
		let tag = tag.clone();
		let user = user.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::list_effective_tag_permissions_for_user_sqlite_sync(transaction, &user, &tag)
			})
			.await
	}

	pub(super) async fn namespace_has_all_read_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		let namespace = namespace.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::namespace_has_all_read_sqlite_sync(transaction, &namespace)
			})
			.await
	}

	pub(crate) async fn tag_has_all_read_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let tag = tag.clone();
		transaction
			.with(move |transaction, _cache| Self::tag_has_all_read_sqlite_sync(transaction, &tag))
			.await
	}

	pub(crate) async fn user_has_tag_permission_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		let tag = tag.clone();
		let user = user.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::user_has_tag_permission_sqlite_sync(transaction, &user, &tag, permission)
			})
			.await
	}

	fn list_effective_namespace_permissions_for_user_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<tg::Permission>> {
		let mut permissions = BTreeSet::new();
		let user = user.to_string();
		for namespace_id in Self::get_namespace_ancestor_ids_sqlite_sync(transaction, namespace)? {
			let statement = indoc!(
				r#"
					select namespace_grants.permission
					from namespace_grants
					where namespace_grants.namespace = ?1
						and (
							namespace_grants.principal = ?2
							or namespace_grants.principal = 'all'
							or exists (
								select 1
								from group_members
								where group_members."group" = namespace_grants.principal
									and group_members."user" = ?2
							)
						);
				"#
			);
			let rows = permissions_sqlite_sync(
				transaction,
				statement,
				sqlite::params![namespace_id, user],
			)?;
			insert_implied_permissions(&mut permissions, rows);
		}
		Ok(permissions.into_iter().collect())
	}

	fn list_effective_tag_permissions_for_user_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
	) -> tg::Result<Vec<tg::Permission>> {
		let mut permissions = Self::list_effective_namespace_permissions_for_user_sqlite_sync(
			transaction,
			user,
			&tag.namespace,
		)?
		.into_iter()
		.collect::<BTreeSet<_>>();
		let Some(namespace_id) =
			Self::try_get_namespace_id_sqlite_sync(transaction, &tag.namespace)?
		else {
			return Ok(permissions.into_iter().collect());
		};
		let statement = indoc!(
			r#"
				select tag_grants.permission
				from tag_grants
				where tag_grants.namespace = ?1
					and tag_grants.name = ?2
					and (
						tag_grants.principal = ?3
						or tag_grants.principal = 'all'
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants.principal
								and group_members."user" = ?3
						)
					);
			"#
		);
		let user = user.to_string();
		let rows = permissions_sqlite_sync(
			transaction,
			statement,
			sqlite::params![namespace_id, tag.name.to_string(), user],
		)?;
		insert_implied_permissions(&mut permissions, rows);
		Ok(permissions.into_iter().collect())
	}

	pub(crate) fn user_has_exact_tag_permission_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		let Some(namespace_id) =
			Self::try_get_namespace_id_sqlite_sync(transaction, &tag.namespace)?
		else {
			return Ok(false);
		};
		let statement = indoc!(
			r#"
				select tag_grants.permission
				from tag_grants
				where tag_grants.namespace = ?1
					and tag_grants.name = ?2
					and (
						tag_grants.principal = ?3
						or tag_grants.principal = 'all'
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants.principal
								and group_members."user" = ?3
						)
					);
			"#
		);
		let rows = permissions_sqlite_sync(
			transaction,
			statement,
			sqlite::params![namespace_id, tag.name.to_string(), user.to_string()],
		)?;
		Ok(rows.into_iter().any(|row| row.implies(permission)))
	}

	pub(crate) fn namespace_has_all_read_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		for namespace_id in Self::get_namespace_ancestor_ids_sqlite_sync(transaction, namespace)? {
			let statement = indoc!(
				r"
					select 1
					from namespace_grants
					where namespace = ?1 and principal = 'all' and permission = 'read';
				"
			);
			let mut statement = transaction
				.prepare(statement)
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			if statement
				.exists(sqlite::params![namespace_id])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			{
				return Ok(true);
			}
		}
		Ok(false)
	}

	fn tag_has_all_read_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		if Self::namespace_has_all_read_sqlite_sync(transaction, &tag.namespace)? {
			return Ok(true);
		}
		Self::tag_has_exact_all_read_sqlite_sync(transaction, tag)
	}

	pub(crate) fn tag_has_exact_all_read_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let Some(namespace_id) =
			Self::try_get_namespace_id_sqlite_sync(transaction, &tag.namespace)?
		else {
			return Ok(false);
		};
		let statement = indoc!(
			r"
				select 1
				from tag_grants
				where namespace = ?1 and name = ?2 and principal = 'all' and permission = 'read';
			"
		);
		let mut statement = transaction
			.prepare(statement)
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		statement
			.exists(sqlite::params![namespace_id, tag.name.to_string()])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	pub(crate) fn user_has_namespace_permission_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		let permissions = Self::list_effective_namespace_permissions_for_user_sqlite_sync(
			transaction,
			user,
			namespace,
		)?;
		Ok(permissions.contains(&permission))
	}

	fn user_has_tag_permission_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		let permissions =
			Self::list_effective_tag_permissions_for_user_sqlite_sync(transaction, user, tag)?;
		Ok(permissions.contains(&permission))
	}

	pub(crate) fn increment_namespace_visibility_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
		let principal = principal.to_string();
		let ids = match Self::get_namespace_ancestor_ids_sqlite_sync_retry(transaction, namespace)?
		{
			ControlFlow::Break(ids) => ids,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for namespace_id in ids.into_iter().filter(|id| *id != 0) {
			let statement = indoc!(
				r"
					insert into namespace_visibility (namespace, principal, count)
					values (?1, ?2, 1)
					on conflict (namespace, principal)
					do update set count = namespace_visibility.count + 1;
				"
			);
			let result = transaction
				.execute(statement, sqlite::params![namespace_id, principal.clone()])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn increment_namespace_visibility_for_user_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
		user: &tg::user::Id,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
		Self::increment_namespace_visibility_sqlite_sync(
			transaction,
			namespace,
			&tg::Principal::User(user.clone()),
		)
	}

	pub(crate) fn increment_namespace_visibility_for_all_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
		Self::increment_namespace_visibility_sqlite_sync(
			transaction,
			namespace,
			&tg::Principal::All,
		)
	}

	pub(crate) fn decrement_namespace_visibility_for_grant_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
		user: Option<&str>,
		group: Option<&str>,
		all: bool,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
		let ids = match Self::get_namespace_ancestor_ids_sqlite_sync_retry(transaction, namespace)?
		{
			ControlFlow::Break(ids) => ids,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for namespace_id in ids.into_iter().filter(|id| *id != 0) {
			let principal = if let Some(user) = user {
				user
			} else if let Some(group) = group {
				group
			} else if all {
				"all"
			} else {
				continue;
			};
			let params: Vec<sqlite::types::Value> =
				vec![namespace_id.into(), principal.to_owned().into()];
			let result = transaction
				.execute(
					"delete from namespace_visibility where namespace = ?1 and principal = ?2 and count = 1;",
					sqlite::params_from_iter(params.iter()),
				)
				.map_err(db::sqlite::Error::from);
			let deleted = crate::database::retry!(result, "failed to execute the statement");
			if deleted == 0 {
				let result = transaction
					.execute(
						"update namespace_visibility set count = count - 1 where namespace = ?1 and principal = ?2 and count > 1;",
						sqlite::params_from_iter(params.iter()),
					)
					.map_err(db::sqlite::Error::from);
				crate::database::retry!(result, "failed to execute the statement");
			}
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn get_namespace_ancestor_ids_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<i64>> {
		match Self::get_namespace_ancestor_ids_sqlite_sync_retry(transaction, namespace)? {
			ControlFlow::Break(ids) => Ok(ids),
			ControlFlow::Continue(error) => Err(tg::error!(!error, "database error")),
		}
	}

	fn get_namespace_ancestor_ids_sqlite_sync_retry(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<Vec<i64>, db::sqlite::Error>> {
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
			return Ok(ControlFlow::Break(ids));
		}
		let placeholders = vec!["?"; names.len()].join(", ");
		let statement = format!(
			"
				select id
				from namespaces
				where name in ({placeholders});
			",
		);
		let result = transaction
			.prepare(&statement)
			.map_err(db::sqlite::Error::from);
		let mut statement = crate::database::retry!(result, "failed to prepare the statement");
		let result = statement
			.query(sqlite::params_from_iter(names.iter()))
			.map_err(db::sqlite::Error::from);
		let mut rows = crate::database::retry!(result, "failed to execute the statement");
		loop {
			let result = rows.next().map_err(db::sqlite::Error::from);
			let Some(row) = crate::database::retry!(result, "failed to get the next row") else {
				break;
			};
			let result = row.get(0).map_err(db::sqlite::Error::from);
			let id = crate::database::retry!(result, "failed to get the id column");
			ids.push(id);
		}
		Ok(ControlFlow::Break(ids))
	}

	pub(crate) fn try_get_namespace_id_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<i64>> {
		if namespace.is_root() {
			return Ok(Some(0));
		}
		let statement = indoc!(
			"
				select id
				from namespaces
				where name = ?1;
			"
		);
		let mut statement = transaction
			.prepare(statement)
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![namespace.to_string()])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to get the next row"))?
		else {
			return Ok(None);
		};
		let id = row
			.get(0)
			.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
		Ok(Some(id))
	}
}

fn insert_implied_permissions(
	permissions: &mut BTreeSet<tg::Permission>,
	grants: impl IntoIterator<Item = tg::Permission>,
) {
	for grant in grants {
		for permission in [
			tg::Permission::Admin,
			tg::Permission::Read,
			tg::Permission::Write,
		] {
			if grant.implies(permission) {
				permissions.insert(permission);
			}
		}
	}
}

fn permissions_sqlite_sync<P>(
	transaction: &sqlite::Transaction<'_>,
	statement: &str,
	params: P,
) -> tg::Result<Vec<tg::Permission>>
where
	P: sqlite::Params,
{
	let mut statement = transaction
		.prepare(statement)
		.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
	let mut rows = statement
		.query(params)
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
	let mut permissions = Vec::new();
	while let Some(row) = rows
		.next()
		.map_err(|error| tg::error!(!error, "failed to get the next row"))?
	{
		let permission = row
			.get::<_, String>(0)
			.map_err(|error| tg::error!(!error, "failed to get the permission column"))?
			.parse()
			.map_err(|error| tg::error!(!error, "invalid permission"))?;
		permissions.push(permission);
	}
	Ok(permissions)
}
