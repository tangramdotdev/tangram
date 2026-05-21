use {
	super::NamespaceReadSubject,
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_database as db,
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

	pub(super) async fn list_effective_tag_permissions_for_user_sqlite(
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

	pub(super) async fn user_has_exact_tag_permission_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		let tag = tag.clone();
		let user = user.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::user_has_exact_tag_permission_sqlite_sync(
					transaction,
					&user,
					&tag,
					permission,
				)
			})
			.await
	}

	pub(super) async fn namespace_has_public_read_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		let namespace = namespace.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::namespace_has_public_read_sqlite_sync(transaction, &namespace)
			})
			.await
	}

	pub(super) async fn tag_has_public_read_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let tag = tag.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::tag_has_public_read_sqlite_sync(transaction, &tag)
			})
			.await
	}

	pub(super) async fn tag_has_exact_public_read_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let tag = tag.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::tag_has_exact_public_read_sqlite_sync(transaction, &tag)
			})
			.await
	}

	pub(super) async fn filter_list_entries_by_read_permission_sqlite(
		transaction: &db::sqlite::Transaction<'_>,
		subject: &NamespaceReadSubject,
		data: Vec<tg::list::Entry>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let subject = subject.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::filter_list_entries_by_read_permission_sqlite_sync(
					transaction,
					&subject,
					data,
				)
			})
			.await
	}

	pub(super) async fn user_has_tag_permission_sqlite(
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
							namespace_grants."user" = ?2
							or namespace_grants."public"
							or exists (
								select 1
								from group_members
								where group_members."group" = namespace_grants."group"
									and group_members."user" = ?2
							)
						) ;
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
						tag_grants."user" = ?3
						or tag_grants."public"
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants."group"
								and group_members."user" = ?3
						)
					) ;
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

	fn user_has_exact_tag_permission_sqlite_sync(
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
						tag_grants."user" = ?3
						or tag_grants."public"
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants."group"
								and group_members."user" = ?3
						)
					) ;
			"#
		);
		let rows = permissions_sqlite_sync(
			transaction,
			statement,
			sqlite::params![namespace_id, tag.name.to_string(), user.to_string()],
		)?;
		Ok(rows.into_iter().any(|row| row.implies(permission)))
	}

	fn namespace_has_public_read_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		for namespace_id in Self::get_namespace_ancestor_ids_sqlite_sync(transaction, namespace)? {
			let statement = indoc!(
				r#"
					select 1
					from namespace_grants
					where namespace = ?1 and "public" and permission = 'read' ;
				"#
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

	fn tag_has_public_read_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		if Self::namespace_has_public_read_sqlite_sync(transaction, &tag.namespace)? {
			return Ok(true);
		}
		Self::tag_has_exact_public_read_sqlite_sync(transaction, tag)
	}

	fn tag_has_exact_public_read_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let Some(namespace_id) =
			Self::try_get_namespace_id_sqlite_sync(transaction, &tag.namespace)?
		else {
			return Ok(false);
		};
		let statement = indoc!(
			r#"
				select 1
				from tag_grants
				where namespace = ?1 and name = ?2 and "public" and permission = 'read' ;
			"#
		);
		let mut statement = transaction
			.prepare(statement)
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		statement
			.exists(sqlite::params![namespace_id, tag.name.to_string()])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	fn filter_list_entries_by_read_permission_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		subject: &NamespaceReadSubject,
		data: Vec<tg::list::Entry>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let mut filtered = Vec::new();
		let mut readable_by_namespace = BTreeMap::new();
		let mut readable_by_tag = BTreeMap::new();
		for entry in data {
			let readable = match &entry {
				tg::list::Entry::Namespace { namespace, .. } => {
					if let Some(readable) = readable_by_namespace.get(namespace) {
						*readable
					} else {
						let readable = Self::namespace_is_readable_sqlite_sync(
							transaction,
							subject,
							namespace,
						)?;
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
								let readable = Self::namespace_is_readable_sqlite_sync(
									transaction,
									subject,
									&tag.namespace,
								)?;
								readable_by_namespace.insert(tag.namespace.clone(), readable);
								readable
							};
						let readable = namespace_readable
							|| Self::tag_is_exactly_readable_sqlite_sync(
								transaction,
								subject,
								tag,
							)?;
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

	fn user_has_namespace_permission_sqlite_sync(
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

	fn namespace_is_readable_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		subject: &NamespaceReadSubject,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		match subject {
			NamespaceReadSubject::All => Ok(true),
			NamespaceReadSubject::Public => {
				Self::namespace_has_public_read_sqlite_sync(transaction, namespace)
			},
			NamespaceReadSubject::User(user) => Self::user_has_namespace_permission_sqlite_sync(
				transaction,
				user,
				namespace,
				tg::Permission::Read,
			),
		}
	}

	fn tag_is_exactly_readable_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		subject: &NamespaceReadSubject,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		match subject {
			NamespaceReadSubject::All => Ok(true),
			NamespaceReadSubject::Public => {
				Self::tag_has_exact_public_read_sqlite_sync(transaction, tag)
			},
			NamespaceReadSubject::User(user) => Self::user_has_exact_tag_permission_sqlite_sync(
				transaction,
				user,
				tag,
				tg::Permission::Read,
			),
		}
	}

	fn get_namespace_ancestor_ids_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
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
		let placeholders = vec!["?"; names.len()].join(", ");
		let statement = format!(
			"
				select id
				from namespaces
				where name in ({placeholders}) ;
			",
		);
		let mut statement = transaction
			.prepare(&statement)
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params_from_iter(names.iter()))
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		while let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to get the next row"))?
		{
			let id = row
				.get(0)
				.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
			ids.push(id);
		}
		Ok(ids)
	}

	fn try_get_namespace_id_sqlite_sync(
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
				where name = ?1 ;
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
