use {
	crate::{Session, context::Authentication, database::Transaction},
	indoc::formatdoc,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

enum NamespaceReadSubject {
	All,
	Public,
	User(tg::user::Id),
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
			"
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

	pub(crate) async fn get_namespace_ancestor_ids_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<i64>> {
		let mut ids = vec![0];
		let p = transaction.p();
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
				ids.push(id);
			}
		}
		Ok(ids)
	}

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

	pub(crate) async fn grant_namespace_public_read_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		namespace_id: i64,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<tg::Grant> {
		let p = transaction.p();
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let created_by = created_by.map(ToString::to_string);
		let statement = formatdoc!(
			"
				insert into namespace_grants (namespace, \"public\", permission, created_at, created_by)
				select {p}1, true, 'read', {p}2, {p}3
				where not exists (
					select 1
					from namespace_grants
					where namespace = {p}1 and \"public\" and permission = 'read'
				);
			"
		);
		let params = db::params![namespace_id, created_at, created_by];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Self::get_public_namespace_grant_with_transaction(transaction, namespace, namespace_id)
			.await
	}

	pub(crate) async fn revoke_namespace_from_user_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		user: &tg::user::Id,
		permission: tg::Permission,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from namespace_grants
				where namespace = {p}1 and \"user\" = {p}2 and permission = {p}3;
			"
		);
		let params = db::params![namespace_id, user.to_string(), permission.to_string()];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn revoke_namespace_from_group_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		group: &tg::group::Id,
		permission: tg::Permission,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from namespace_grants
				where namespace = {p}1 and \"group\" = {p}2 and permission = {p}3;
			"
		);
		let params = db::params![namespace_id, group.to_string(), permission.to_string()];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn revoke_public_namespace_read_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from namespace_grants
				where namespace = {p}1 and \"public\" and permission = 'read';
			"
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
			"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.\"user\" as \"user\",
					namespace_grants.\"group\" as \"group\",
					namespace_grants.\"public\" as public,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.\"user\" = {p}1
				order by namespace_grants.namespace, namespace_grants.permission;
			"
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
			"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.\"user\" as \"user\",
					namespace_grants.\"group\" as \"group\",
					namespace_grants.\"public\" as public,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.\"group\" = {p}1
				order by namespace_grants.namespace, namespace_grants.permission;
			"
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
			"
				select
					coalesce(namespaces.name, '') as namespace,
					namespace_grants.\"user\" as \"user\",
					namespace_grants.\"group\" as \"group\",
					namespace_grants.\"public\" as public,
					namespace_grants.permission,
					namespace_grants.created_at,
					namespace_grants.created_by
				from namespace_grants
				left join namespaces on namespaces.id = namespace_grants.namespace
				where namespace_grants.namespace = {p}1
				order by namespace_grants.\"user\", namespace_grants.\"group\", namespace_grants.permission;
			"
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

	pub(crate) async fn list_effective_namespace_permissions_for_user_with_transaction(
		transaction: &Transaction<'_>,
		user: &tg::user::Id,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<tg::Permission>> {
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
				"
					select namespace_grants.permission
					from namespace_grants
					where namespace_grants.namespace = {p}1
						and (
							namespace_grants.\"user\" = {p}2
							or namespace_grants.\"public\"
							or exists (
								select 1
								from group_members
								where group_members.\"group\" = namespace_grants.\"group\"
									and group_members.\"user\" = {p}2
							)
						);
				"
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

	pub(crate) async fn namespace_has_public_read_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		let p = transaction.p();
		for namespace_id in
			Self::get_namespace_ancestor_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				"
					select 1
					from namespace_grants
					where namespace = {p}1 and \"public\" and permission = 'read';
				"
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

	fn namespace_read_subject(&self) -> tg::Result<NamespaceReadSubject> {
		if let Some(user) = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
		{
			Ok(NamespaceReadSubject::User(user.id.clone()))
		} else if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_root)
		{
			Ok(NamespaceReadSubject::All)
		} else if self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_sandbox_ref().ok())
			.is_some()
		{
			Ok(NamespaceReadSubject::Public)
		} else {
			if self.context.token.is_some() {
				return Err(tg::error!("failed to authorize"));
			}
			Ok(NamespaceReadSubject::Public)
		}
	}

	pub(crate) fn authorize_list(&self) -> tg::Result<()> {
		self.namespace_read_subject().map(|_| ())
	}

	pub(crate) async fn authorize_namespace(
		&self,
		namespace: &tg::Namespace,
		permission: tg::Permission,
	) -> tg::Result<()> {
		if permission != tg::Permission::Read {
			if self
				.context
				.authentication
				.as_ref()
				.is_some_and(Authentication::is_root)
			{
				return Ok(());
			}
			let user = self
				.context
				.authentication
				.as_ref()
				.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
				.ok_or_else(|| tg::error!("failed to authorize"))?;
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
			return Err(tg::error!("forbidden"));
		}

		let subject = self.namespace_read_subject()?;
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
					Err(tg::error!("forbidden"))
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
					Err(tg::error!("forbidden"))
				}
			},
		}
	}

	pub(crate) async fn filter_list_entries_by_read_permission(
		&self,
		data: Vec<tg::list::Entry>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let subject = self.namespace_read_subject()?;
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
		let mut filtered = Vec::new();
		let mut readable_by_namespace = BTreeMap::new();
		for entry in data {
			let namespace = match &entry {
				tg::list::Entry::Namespace { namespace, .. } => namespace.clone(),
				tg::list::Entry::Tag { tag, .. } => tag.namespace.clone(),
			};
			let readable = if let Some(readable) = readable_by_namespace.get(&namespace) {
				*readable
			} else {
				let readable = match &subject {
					NamespaceReadSubject::All => true,
					NamespaceReadSubject::Public => {
						Self::namespace_has_public_read_with_transaction(&transaction, &namespace)
							.await?
					},
					NamespaceReadSubject::User(user) => {
						Self::user_has_namespace_permission_with_transaction(
							&transaction,
							user,
							&namespace,
							tg::Permission::Read,
						)
						.await?
					},
				};
				readable_by_namespace.insert(namespace, readable);
				readable
			};
			if readable {
				filtered.push(entry);
			}
		}
		Ok(filtered)
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
			public: false,
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
			public: false,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	async fn get_public_namespace_grant_with_transaction(
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
			"
				select created_at, created_by
				from namespace_grants
				where namespace = {p}1 and \"public\" and permission = 'read';
			"
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
}
