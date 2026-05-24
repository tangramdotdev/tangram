use {
	crate::{Session, context::Authentication},
	indoc::indoc,
	rusqlite as sqlite,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) fn filter_list_entries_by_access_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		authentication: Option<&Authentication>,
		data: Vec<tg::list::Entry>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let mut filtered = Vec::new();
		let mut readable_by_namespace = BTreeMap::new();
		let mut visible_by_namespace = BTreeMap::new();
		let mut readable_by_tag = BTreeMap::new();
		for entry in data {
			let visible = match &entry {
				tg::list::Entry::Namespace { namespace, .. } => {
					if let Some(visible) = visible_by_namespace.get(namespace) {
						*visible
					} else {
						let visible = Self::namespace_is_visible_for_list_sqlite_sync(
							transaction,
							authentication,
							namespace,
						)?;
						visible_by_namespace.insert(namespace.clone(), visible);
						visible
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
								let readable = Self::namespace_is_readable_for_list_sqlite_sync(
									transaction,
									authentication,
									&tag.namespace,
								)?;
								readable_by_namespace.insert(tag.namespace.clone(), readable);
								readable
							};
						let readable = namespace_readable
							|| Self::tag_is_exactly_readable_for_list_sqlite_sync(
								transaction,
								authentication,
								tag,
							)?;
						readable_by_tag.insert(tag.clone(), readable);
						readable
					}
				},
			};
			if visible {
				filtered.push(entry);
			}
		}
		Ok(filtered)
	}

	fn namespace_is_readable_for_list_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		authentication: Option<&Authentication>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		if Authentication::uses_all_grants(authentication) {
			return Self::namespace_has_all_read_sqlite_sync(transaction, namespace);
		}
		match authentication {
			Some(Authentication::Root) => Ok(true),
			Some(Authentication::User(user)) => Self::user_has_namespace_permission_sqlite_sync(
				transaction,
				&user.id,
				namespace,
				tg::Permission::Read,
			),
			_ => unreachable!(),
		}
	}

	fn namespace_is_visible_for_list_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		authentication: Option<&Authentication>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		if Self::namespace_is_readable_for_list_sqlite_sync(transaction, authentication, namespace)?
		{
			return Ok(true);
		}
		let Some(namespace_id) = Self::try_get_namespace_id_sqlite_sync(transaction, namespace)?
		else {
			return Ok(false);
		};
		let exists = if Authentication::uses_all_grants(authentication) {
			let statement = indoc!(
				r#"
					select 1
					from namespace_visibility
					where namespace = ?1 and "all" ;
				"#
			);
			let mut statement = transaction
				.prepare(statement)
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			statement
				.exists(sqlite::params![namespace_id])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		} else if let Some(user) = Authentication::user_id(authentication) {
			let statement = indoc!(
				r#"
						select 1
						from namespace_visibility
						where namespace = ?1
							and (
								"user" = ?2
								or "all"
								or exists (
									select 1
									from group_members
									where group_members."group" = namespace_visibility."group"
										and group_members."user" = ?2
								)
							) ;
					"#
			);
			let mut statement = transaction
				.prepare(statement)
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			statement
				.exists(sqlite::params![namespace_id, user.to_string()])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		} else {
			true
		};
		Ok(exists)
	}

	fn tag_is_exactly_readable_for_list_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		authentication: Option<&Authentication>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		if Authentication::uses_all_grants(authentication) {
			return Self::tag_has_exact_all_read_sqlite_sync(transaction, tag);
		}
		match authentication {
			Some(Authentication::Root) => Ok(true),
			Some(Authentication::User(user)) => Self::user_has_exact_tag_permission_sqlite_sync(
				transaction,
				&user.id,
				tag,
				tg::Permission::Read,
			),
			_ => unreachable!(),
		}
	}
}
