use {
	crate::{Session, context::Authentication},
	indoc::indoc,
	rusqlite as sqlite,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) fn filter_list_entries_by_visibility_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
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
					if let Some(visible_by_permission) = visible_by_permission_by_tag.get(tag) {
						*visible_by_permission
					} else {
						let namespace_visible_by_permission = if let Some(visible_by_permission) =
							visible_by_permission_by_namespace.get(&tag.namespace)
						{
							*visible_by_permission
						} else {
							let visible_by_permission =
								Self::namespace_is_visible_by_permission_for_list_sqlite_sync(
									transaction,
									authentication,
									&tag.namespace,
								)?;
							visible_by_permission_by_namespace
								.insert(tag.namespace.clone(), visible_by_permission);
							visible_by_permission
						};
						let visible = namespace_visible_by_permission
							|| Self::tag_is_visible_by_permission_for_list_sqlite_sync(
								transaction,
								authentication,
								tag,
							)?;
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

	fn namespace_is_visible_by_permission_for_list_sqlite_sync(
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
		if Self::namespace_is_visible_by_permission_for_list_sqlite_sync(
			transaction,
			authentication,
			namespace,
		)? {
			return Ok(true);
		}
		let Some(namespace_id) = Self::try_get_namespace_id_sqlite_sync(transaction, namespace)?
		else {
			return Ok(false);
		};
		let exists = if Authentication::uses_all_grants(authentication) {
			let statement = indoc!(
				r"
					select 1
					from namespace_visibility
					where namespace = ?1 and principal = 'all' ;
				"
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
								principal = ?2
								or principal = 'all'
								or exists (
									select 1
									from group_members
									where group_members."group" = namespace_visibility.principal
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

	fn tag_is_visible_by_permission_for_list_sqlite_sync(
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
