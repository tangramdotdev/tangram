use {
	crate::{Session, context::Authentication},
	indoc::indoc,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database as db,
};

impl Session {
	pub(crate) async fn filter_list_entries_by_visibility_postgres(
		transaction: &db::postgres::Transaction<'_>,
		authentication: Option<&Authentication>,
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
		let visible_by_permission_namespaces = visible_by_permission_namespaces_postgres(
			transaction,
			authentication,
			&candidate_namespaces,
		)
		.await?;
		let visible_namespaces = visible_namespaces_postgres(
			transaction,
			authentication,
			&candidate_namespaces,
			visible_by_permission_namespaces.clone(),
		)
		.await?;
		let visible_tags =
			visible_tags_postgres(transaction, authentication, &candidate_tags).await?;
		Ok(data
			.into_iter()
			.filter(|entry| match entry {
				tg::list::Entry::Namespace { namespace, .. } => {
					visible_namespaces.contains(namespace)
				},
				tg::list::Entry::Tag { tag, .. } => {
					visible_by_permission_namespaces.contains(&tag.namespace)
						|| visible_tags.contains(tag)
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

async fn visible_by_permission_namespaces_postgres(
	transaction: &db::postgres::Transaction<'_>,
	authentication: Option<&Authentication>,
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

	let rows = match authentication {
		Some(Authentication::Root) => return Ok(namespaces.clone()),
		None
		| Some(Authentication::Process(_) | Authentication::Runner | Authentication::Sandbox(_)) => {
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
		Some(Authentication::User(user)) => {
			let user = user.id.to_string();
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
					),
					matching_principals("user", "group", "all") as (
						values ($3::text, null::text, false), (null::text, null::text, true)
						union all
						select null::text, group_members."group", false
						from group_members
						where group_members."user" = $3
					)
					select distinct ancestor_ids.candidate
					from ancestor_ids
					join namespace_grants on namespace_grants.namespace = ancestor_ids.id
					where exists (
						select 1
						from matching_principals
						where namespace_grants."user" = matching_principals."user"
							or namespace_grants."group" = matching_principals."group"
							or (namespace_grants."all" and matching_principals."all")
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

async fn visible_namespaces_postgres(
	transaction: &db::postgres::Transaction<'_>,
	authentication: Option<&Authentication>,
	namespaces: &BTreeSet<tg::Namespace>,
	visible_by_permission: BTreeSet<tg::Namespace>,
) -> tg::Result<BTreeSet<tg::Namespace>> {
	let mut visible = visible_by_permission;
	let candidates = namespaces
		.iter()
		.map(ToString::to_string)
		.collect::<Vec<_>>();
	if candidates.is_empty() {
		return Ok(visible);
	}

	let rows = match authentication {
		Some(Authentication::Root) => return Ok(namespaces.clone()),
		None
		| Some(Authentication::Process(_) | Authentication::Runner | Authentication::Sandbox(_)) => {
			let statement = indoc!(
				r#"
					with candidate_namespaces(candidate) as (
						select *
						from unnest($1::text[])
					),
					candidate_ids(candidate, id) as (
						select candidate, 0::bigint
						from candidate_namespaces
						where candidate = ''
						union all
						select candidate_namespaces.candidate, namespaces.id
						from candidate_namespaces
						join namespaces on namespaces.name = candidate_namespaces.candidate
					)
					select distinct candidate_ids.candidate
					from candidate_ids
					join namespace_visibility on namespace_visibility.namespace = candidate_ids.id
					where namespace_visibility."all";
				"#
			);
			transaction
				.inner()
				.query(statement, &[&candidates])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		},
		Some(Authentication::User(user)) => {
			let user = user.id.to_string();
			let statement = indoc!(
				r#"
					with candidate_namespaces(candidate) as (
						select *
						from unnest($1::text[])
					),
					candidate_ids(candidate, id) as (
						select candidate, 0::bigint
						from candidate_namespaces
						where candidate = ''
						union all
						select candidate_namespaces.candidate, namespaces.id
						from candidate_namespaces
						join namespaces on namespaces.name = candidate_namespaces.candidate
					),
					matching_principals("user", "group", "all") as (
						values ($2::text, null::text, false), (null::text, null::text, true)
						union all
						select null::text, group_members."group", false
						from group_members
						where group_members."user" = $2
					)
					select distinct candidate_ids.candidate
					from candidate_ids
					join namespace_visibility on namespace_visibility.namespace = candidate_ids.id
					where exists (
						select 1
						from matching_principals
						where namespace_visibility."user" = matching_principals."user"
							or namespace_visibility."group" = matching_principals."group"
							or (namespace_visibility."all" and matching_principals."all")
					);
				"#
			);
			transaction
				.inner()
				.query(statement, &[&candidates, &user])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		},
	};

	for row in rows {
		let namespace: String = row
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the namespace column"))?;
		visible.insert(
			namespace
				.parse()
				.map_err(|error| tg::error!(!error, "invalid namespace"))?,
		);
	}
	Ok(visible)
}

async fn visible_tags_postgres(
	transaction: &db::postgres::Transaction<'_>,
	authentication: Option<&Authentication>,
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

	let rows = match authentication {
		Some(Authentication::Root) => return Ok(tags.clone()),
		None
		| Some(Authentication::Process(_) | Authentication::Runner | Authentication::Sandbox(_)) => {
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
		Some(Authentication::User(user)) => {
			let user = user.id.to_string();
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
					),
					matching_principals("user", "group", "all") as (
						values ($4::text, null::text, false), (null::text, null::text, true)
						union all
						select null::text, group_members."group", false
						from group_members
						where group_members."user" = $4
					)
					select distinct tag_namespace.tag
					from tag_namespace
					join tag_grants
						on tag_grants.namespace = tag_namespace.namespace_id
						and tag_grants.name = tag_namespace.name
					where exists (
						select 1
						from matching_principals
						where tag_grants."user" = matching_principals."user"
							or tag_grants."group" = matching_principals."group"
							or (tag_grants."all" and matching_principals."all")
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
