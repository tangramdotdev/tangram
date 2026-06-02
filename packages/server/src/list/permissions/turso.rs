use {
	crate::{Session, context::Authentication},
	indoc::{formatdoc, indoc},
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn filter_list_entries_by_visibility_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		authentication: Option<&Authentication>,
		data: Vec<tg::list::Entry>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let mut filtered = Vec::new();
		let mut visible_by_permission_by_namespace = BTreeMap::new();
		let mut visible_by_namespace = BTreeMap::new();
		let mut visible_by_permission_by_tag = BTreeMap::new();
		for entry in data {
			let visible = match &entry {
				tg::list::Entry::Namespace { namespace, .. } => {
					if let Some(visible) = visible_by_namespace.get(namespace) {
						*visible
					} else {
						let visible = Self::namespace_is_visible_for_list_turso_with_transaction(
							transaction,
							authentication,
							namespace,
						)
						.await?;
						visible_by_namespace.insert(namespace.clone(), visible);
						visible
					}
				},
				tg::list::Entry::Tag { tag, .. } => {
					if let Some(visible_by_permission) = visible_by_permission_by_tag.get(tag) {
						*visible_by_permission
					} else {
						let namespace_visible_by_permission = if let Some(visible_by_permission) =
							visible_by_permission_by_namespace.get(&tag.namespace)
						{
							*visible_by_permission
						} else {
							let visible_by_permission =
								Self::namespace_is_visible_by_permission_for_list_turso_with_transaction(
									transaction,
									authentication,
									&tag.namespace,
								)
								.await?;
							visible_by_permission_by_namespace
								.insert(tag.namespace.clone(), visible_by_permission);
							visible_by_permission
						};
						let visible = namespace_visible_by_permission
							|| Self::tag_is_visible_by_permission_for_list_turso_with_transaction(
								transaction,
								authentication,
								tag,
							)
							.await?;
						visible_by_permission_by_tag.insert(tag.clone(), visible);
						visible
					}
				},
			};
			if visible {
				filtered.push(entry);
			}
		}
		Ok(filtered)
	}

	async fn namespace_is_visible_by_permission_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		authentication: Option<&Authentication>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		if Authentication::uses_all_grants(authentication) {
			return namespace_has_all_read_turso_with_transaction(transaction, namespace).await;
		}
		match authentication {
			Some(Authentication::Root) => Ok(true),
			Some(Authentication::User(user)) => {
				user_has_namespace_permission_turso_with_transaction(
					transaction,
					&user.id,
					namespace,
					tg::Permission::Read,
				)
				.await
			},
			_ => unreachable!(),
		}
	}

	async fn namespace_is_visible_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		authentication: Option<&Authentication>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		if Self::namespace_is_visible_by_permission_for_list_turso_with_transaction(
			transaction,
			authentication,
			namespace,
		)
		.await?
		{
			return Ok(true);
		}
		let Some(namespace_id) =
			try_get_namespace_id_turso_with_transaction(transaction, namespace).await?
		else {
			return Ok(false);
		};
		let exists = if Authentication::uses_all_grants(authentication) {
			let statement = indoc!(
				r"
					select 1
					from namespace_visibility
					where namespace = ?1 and principal = 'all';
				"
			);
			transaction
				.query_optional(statement.into(), db::params![namespace_id])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				.is_some()
		} else if let Some(user) = Authentication::user_id(authentication) {
			let statement = indoc!(
				r#"
					select 1
					from namespace_visibility
					where namespace = ?1
						and (
							principal = ?2
							or principal = 'all'
							or exists (
								select 1
								from group_members
								where group_members."group" = namespace_visibility.principal
									and group_members."user" = ?2
							)
						);
				"#
			);
			transaction
				.query_optional(
					statement.into(),
					db::params![namespace_id, user.to_string()],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				.is_some()
		} else {
			true
		};
		Ok(exists)
	}

	async fn tag_is_visible_by_permission_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		authentication: Option<&Authentication>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		if Authentication::uses_all_grants(authentication) {
			return tag_has_all_read_turso_with_transaction(transaction, tag).await;
		}
		match authentication {
			Some(Authentication::Root) => Ok(true),
			Some(Authentication::User(user)) => {
				user_has_tag_permission_turso_with_transaction(
					transaction,
					&user.id,
					tag,
					tg::Permission::Read,
				)
				.await
			},
			_ => unreachable!(),
		}
	}
}

async fn try_get_namespace_id_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	namespace: &tg::Namespace,
) -> tg::Result<Option<i64>> {
	if namespace.is_root() {
		return Ok(Some(0));
	}
	let statement = "
		select id
		from namespaces
		where name = ?1;
	";
	transaction
		.query_optional_value_into::<i64>(statement.into(), db::params![namespace.to_string()])
		.await
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))
}

async fn get_namespace_ancestor_ids_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
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
	let placeholders = (1..=names.len())
		.map(|i| format!("?{i}"))
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

async fn namespace_has_all_read_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	namespace: &tg::Namespace,
) -> tg::Result<bool> {
	for namespace_id in
		get_namespace_ancestor_ids_turso_with_transaction(transaction, namespace).await?
	{
		let statement = "
			select 1
			from namespace_grants
			where namespace = ?1 and principal = 'all' and permission = 'read';
		";
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

async fn user_has_namespace_permission_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	user: &tg::user::Id,
	namespace: &tg::Namespace,
	permission: tg::Permission,
) -> tg::Result<bool> {
	let permissions = list_effective_namespace_permissions_for_user_turso_with_transaction(
		transaction,
		user,
		namespace,
	)
	.await?;
	Ok(permissions.contains(&permission))
}

async fn list_effective_namespace_permissions_for_user_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	user: &tg::user::Id,
	namespace: &tg::Namespace,
) -> tg::Result<Vec<tg::Permission>> {
	#[derive(db::row::Deserialize)]
	struct Row {
		#[tangram_database(as = "db::value::FromStr")]
		permission: tg::Permission,
	}

	let mut permissions = BTreeSet::new();
	for namespace_id in
		get_namespace_ancestor_ids_turso_with_transaction(transaction, namespace).await?
	{
		let statement = formatdoc!(
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
		let rows = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![namespace_id, user.to_string()],
			)
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

async fn user_has_tag_permission_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	user: &tg::user::Id,
	tag: &tg::Tag,
	permission: tg::Permission,
) -> tg::Result<bool> {
	let permissions =
		list_effective_tag_permissions_for_user_turso_with_transaction(transaction, user, tag)
			.await?;
	Ok(permissions.contains(&permission))
}

async fn list_effective_tag_permissions_for_user_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	user: &tg::user::Id,
	tag: &tg::Tag,
) -> tg::Result<Vec<tg::Permission>> {
	#[derive(db::row::Deserialize)]
	struct Row {
		#[tangram_database(as = "db::value::FromStr")]
		permission: tg::Permission,
	}

	let mut permissions = list_effective_namespace_permissions_for_user_turso_with_transaction(
		transaction,
		user,
		&tag.namespace,
	)
	.await?
	.into_iter()
	.collect::<BTreeSet<_>>();
	if let Some(namespace_id) =
		try_get_namespace_id_turso_with_transaction(transaction, &tag.namespace).await?
	{
		let statement = formatdoc!(
			r#"
				with matching_principals(principal) as (
					values (?3), ('all')
					union all
					select group_members."group"
					from group_members
					where group_members."user" = ?3
				)
				select tag_grants.permission
				from tag_grants
				where tag_grants.namespace = ?1
					and tag_grants.name = ?2
					and exists (
						select 1
						from matching_principals
						where tag_grants.principal = matching_principals.principal
					);
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![namespace_id, tag.name.to_string(), user.to_string()],
			)
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

async fn tag_has_all_read_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	tag: &tg::Tag,
) -> tg::Result<bool> {
	if namespace_has_all_read_turso_with_transaction(transaction, &tag.namespace).await? {
		return Ok(true);
	}
	let Some(namespace_id) =
		try_get_namespace_id_turso_with_transaction(transaction, &tag.namespace).await?
	else {
		return Ok(false);
	};
	let statement = "
		select 1
		from tag_grants
		where namespace = ?1 and name = ?2 and principal = 'all' and permission = 'read';
	";
	Ok(transaction
		.query_optional(
			statement.into(),
			db::params![namespace_id, tag.name.to_string()],
		)
		.await
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		.is_some())
}
