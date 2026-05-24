use {
	super::NamespaceReadSubject, crate::Session, indoc::indoc, std::collections::BTreeSet,
	tangram_client::prelude::*, tangram_database as db,
};

impl Session {
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
				)
				select namespace_grants.permission
				from namespace_grants
				where namespace_grants.namespace in (select id from ancestor_ids)
					and (
						namespace_grants."user" = $2
						or namespace_grants."all"
						or exists (
							select 1
							from group_members
							where group_members."group" = namespace_grants."group"
								and group_members."user" = $2
						)
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
				)
				select namespace_grants.permission
				from namespace_grants
				where namespace_grants.namespace in (select id from ancestor_ids)
					and (
						namespace_grants."user" = $4
						or namespace_grants."all"
						or exists (
							select 1
							from group_members
							where group_members."group" = namespace_grants."group"
								and group_members."user" = $4
						)
					)
				union all
				select tag_grants.permission
				from tag_grants
				where tag_grants.namespace in (select id from tag_namespace)
					and tag_grants.name = $3
					and (
						tag_grants."user" = $4
						or tag_grants."all"
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants."group"
								and group_members."user" = $4
						)
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
			r#"
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
						and namespace_grants."all"
						and namespace_grants.permission = 'read'
				);
			"#
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
				)
				select exists (
					select 1
					from namespace_grants
					where namespace_grants.namespace in (select id from ancestor_ids)
						and namespace_grants."all"
						and namespace_grants.permission = 'read'
				) or exists (
					select 1
					from tag_grants
					where tag_grants.namespace in (select id from tag_namespace)
						and tag_grants.name = $3
						and tag_grants."all"
						and tag_grants.permission = 'read'
				);
			"#
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

	pub(crate) async fn tag_has_exact_all_read_postgres(
		transaction: &db::postgres::Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let namespace = tag.namespace.to_string();
		let name = tag.name.to_string();
		let statement = indoc!(
			r#"
				with tag_namespace(id) as (
					select 0::bigint
					where $1 = ''
					union
					select namespaces.id
					from namespaces
					where namespaces.name = $1
				)
				select exists (
					select 1
					from tag_grants
					where tag_grants.namespace in (select id from tag_namespace)
						and tag_grants.name = $2
						and tag_grants."all"
						and tag_grants.permission = 'read'
				);
			"#
		);
		let rows = transaction
			.inner()
			.query(statement, &[&namespace, &name])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		rows.first()
			.ok_or_else(|| tg::error!("expected a row"))?
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the exists column"))
	}

	pub(crate) async fn user_has_exact_tag_permission_postgres(
		transaction: &db::postgres::Transaction<'_>,
		user: &tg::user::Id,
		tag: &tg::Tag,
		permission: tg::Permission,
	) -> tg::Result<bool> {
		let namespace = tag.namespace.to_string();
		let name = tag.name.to_string();
		let user = user.to_string();
		let statement = indoc!(
			r#"
				with tag_namespace(id) as (
					select 0::bigint
					where $1 = ''
					union
					select namespaces.id
					from namespaces
					where namespaces.name = $1
				)
				select tag_grants.permission
				from tag_grants
				where tag_grants.namespace in (select id from tag_namespace)
					and tag_grants.name = $2
					and (
						tag_grants."user" = $3
						or tag_grants."all"
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants."group"
								and group_members."user" = $3
						)
					);
			"#
		);
		let rows = transaction
			.inner()
			.query(statement, &[&namespace, &name, &user])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		rows.into_iter().try_fold(false, |has_permission, row| {
			let row_permission = parse_permission(
				&row.try_get::<_, String>(0)
					.map_err(|error| tg::error!(!error, "failed to get the permission column"))?,
			)?;
			Ok::<_, tg::Error>(has_permission || row_permission.implies(permission))
		})
	}

	pub(super) async fn filter_list_entries_by_read_permission_postgres(
		transaction: &db::postgres::Transaction<'_>,
		subject: &NamespaceReadSubject,
		data: Vec<tg::list::Entry>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let mut candidate_namespaces = BTreeSet::new();
		let mut candidate_tags = BTreeSet::new();
		for entry in &data {
			match entry {
				tg::list::Entry::Namespace { namespace, .. } => {
					candidate_namespaces.insert(namespace.clone());
				},
				tg::list::Entry::Tag { tag, .. } => {
					candidate_namespaces.insert(tag.namespace.clone());
					candidate_tags.insert(tag.clone());
				},
			}
		}
		let readable_namespaces =
			readable_namespaces_postgres(transaction, subject, &candidate_namespaces).await?;
		let readable_tags = readable_tags_postgres(transaction, subject, &candidate_tags).await?;
		Ok(data
			.into_iter()
			.filter(|entry| match entry {
				tg::list::Entry::Namespace { namespace, .. } => {
					readable_namespaces.contains(namespace)
				},
				tg::list::Entry::Tag { tag, .. } => {
					readable_namespaces.contains(&tag.namespace) || readable_tags.contains(tag)
				},
			})
			.collect())
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

async fn readable_namespaces_postgres(
	transaction: &db::postgres::Transaction<'_>,
	subject: &NamespaceReadSubject,
	namespaces: &BTreeSet<tg::Namespace>,
) -> tg::Result<BTreeSet<tg::Namespace>> {
	let mut candidates = Vec::new();
	let mut ancestors = Vec::new();
	for namespace in namespaces {
		let candidate = namespace.to_string();
		candidates.push(candidate.clone());
		ancestors.push(String::new());
		for ancestor in ancestor_names(namespace) {
			candidates.push(candidate.clone());
			ancestors.push(ancestor);
		}
	}
	if candidates.is_empty() {
		return Ok(BTreeSet::new());
	}

	let rows = match subject {
		NamespaceReadSubject::All => return Ok(namespaces.clone()),
		NamespaceReadSubject::Anonymous => {
			let statement = indoc!(
				r#"
					with candidate_ancestors(candidate, ancestor) as (
						select *
						from unnest($1::text[], $2::text[])
					),
					ancestor_ids(candidate, id) as (
						select candidate_ancestors.candidate, 0::bigint
						from candidate_ancestors
						where candidate_ancestors.ancestor = ''
						union all
						select candidate_ancestors.candidate, namespaces.id
						from candidate_ancestors
						join namespaces on namespaces.name = candidate_ancestors.ancestor
					)
					select distinct ancestor_ids.candidate
					from ancestor_ids
					join namespace_grants on namespace_grants.namespace = ancestor_ids.id
					where namespace_grants."all"
						and namespace_grants.permission = 'read';
				"#
			);
			transaction
				.inner()
				.query(statement, &[&candidates, &ancestors])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		},
		NamespaceReadSubject::User(user) => {
			let user = user.to_string();
			let statement = indoc!(
				r#"
					with candidate_ancestors(candidate, ancestor) as (
						select *
						from unnest($1::text[], $2::text[])
					),
					ancestor_ids(candidate, id) as (
						select candidate_ancestors.candidate, 0::bigint
						from candidate_ancestors
						where candidate_ancestors.ancestor = ''
						union all
						select candidate_ancestors.candidate, namespaces.id
						from candidate_ancestors
						join namespaces on namespaces.name = candidate_ancestors.ancestor
					)
					select distinct ancestor_ids.candidate
					from ancestor_ids
					join namespace_grants on namespace_grants.namespace = ancestor_ids.id
					where namespace_grants."user" = $3
						or namespace_grants."all"
						or exists (
							select 1
							from group_members
							where group_members."group" = namespace_grants."group"
								and group_members."user" = $3
						);
				"#
			);
			transaction
				.inner()
				.query(statement, &[&candidates, &ancestors, &user])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		},
	};

	rows.into_iter()
		.map(|row| {
			let namespace: String = row
				.try_get(0)
				.map_err(|error| tg::error!(!error, "failed to get the namespace column"))?;
			namespace
				.parse()
				.map_err(|error| tg::error!(!error, "invalid namespace"))
		})
		.collect()
}

async fn readable_tags_postgres(
	transaction: &db::postgres::Transaction<'_>,
	subject: &NamespaceReadSubject,
	tags: &BTreeSet<tg::Tag>,
) -> tg::Result<BTreeSet<tg::Tag>> {
	let tag_keys = tags.iter().map(ToString::to_string).collect::<Vec<_>>();
	let namespaces = tags
		.iter()
		.map(|tag| tag.namespace.to_string())
		.collect::<Vec<_>>();
	let names = tags
		.iter()
		.map(|tag| tag.name.to_string())
		.collect::<Vec<_>>();
	if tag_keys.is_empty() {
		return Ok(BTreeSet::new());
	}

	let rows = match subject {
		NamespaceReadSubject::All => return Ok(tags.clone()),
		NamespaceReadSubject::Anonymous => {
			let statement = indoc!(
				r#"
					with candidate_tags(tag, namespace, name) as (
						select *
						from unnest($1::text[], $2::text[], $3::text[])
					),
					tag_namespace(tag, namespace_id, name) as (
						select candidate_tags.tag, 0::bigint, candidate_tags.name
						from candidate_tags
						where candidate_tags.namespace = ''
						union all
						select candidate_tags.tag, namespaces.id, candidate_tags.name
						from candidate_tags
						join namespaces on namespaces.name = candidate_tags.namespace
					)
					select distinct tag_namespace.tag
					from tag_namespace
					join tag_grants
						on tag_grants.namespace = tag_namespace.namespace_id
						and tag_grants.name = tag_namespace.name
					where tag_grants."all"
						and tag_grants.permission = 'read';
				"#
			);
			transaction
				.inner()
				.query(statement, &[&tag_keys, &namespaces, &names])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		},
		NamespaceReadSubject::User(user) => {
			let user = user.to_string();
			let statement = indoc!(
				r#"
					with candidate_tags(tag, namespace, name) as (
						select *
						from unnest($1::text[], $2::text[], $3::text[])
					),
					tag_namespace(tag, namespace_id, name) as (
						select candidate_tags.tag, 0::bigint, candidate_tags.name
						from candidate_tags
						where candidate_tags.namespace = ''
						union all
						select candidate_tags.tag, namespaces.id, candidate_tags.name
						from candidate_tags
						join namespaces on namespaces.name = candidate_tags.namespace
					)
					select distinct tag_namespace.tag
					from tag_namespace
					join tag_grants
						on tag_grants.namespace = tag_namespace.namespace_id
						and tag_grants.name = tag_namespace.name
					where tag_grants."user" = $4
						or tag_grants."all"
						or exists (
							select 1
							from group_members
							where group_members."group" = tag_grants."group"
								and group_members."user" = $4
						);
				"#
			);
			transaction
				.inner()
				.query(statement, &[&tag_keys, &namespaces, &names, &user])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		},
	};

	rows.into_iter()
		.map(|row| {
			let tag: String = row
				.try_get(0)
				.map_err(|error| tg::error!(!error, "failed to get the tag column"))?;
			tag.parse()
				.map_err(|error| tg::error!(!error, "invalid tag"))
		})
		.collect()
}
