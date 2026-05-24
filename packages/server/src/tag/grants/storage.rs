use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
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
		let implies_read = permission.implies(tg::Permission::Read);
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
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if n > 0 && implies_read {
			Self::increment_namespace_visibility_for_user_with_transaction(
				transaction,
				&tag.namespace,
				user,
			)
			.await?;
		}
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
		let implies_read = permission.implies(tg::Permission::Read);
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
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if n > 0 && implies_read {
			Self::increment_namespace_visibility_for_group_with_transaction(
				transaction,
				&tag.namespace,
				group,
			)
			.await?;
		}
		Self::get_tag_grant_for_group_with_transaction(
			transaction,
			tag,
			namespace_id,
			group,
			&permission,
		)
		.await
	}

	pub(crate) async fn create_tag_grant_for_all_with_transaction(
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
				insert into tag_grants (namespace, name, "all", permission, created_at, created_by)
				select {p}1, {p}2, true, 'read', {p}3, {p}4
				where not exists (
					select 1
					from tag_grants
					where namespace = {p}1 and name = {p}2 and "all" and permission = 'read'
				);
			"#
		);
		let params = db::params![namespace_id, tag.name.to_string(), created_at, created_by,];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if n > 0 {
			Self::increment_namespace_visibility_for_all_with_transaction(
				transaction,
				&tag.namespace,
			)
			.await?;
		}
		Self::get_tag_grant_for_all_with_transaction(transaction, tag, namespace_id).await
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
		if n > 0 && permission.implies(tg::Permission::Read) {
			let namespace = Self::namespace_for_id_with_transaction(transaction, namespace_id)
				.await?
				.ok_or_else(|| tg::error!("failed to find the namespace"))?;
			Self::decrement_namespace_visibility_for_user_with_transaction(
				transaction,
				&namespace,
				user,
			)
			.await?;
		}
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
		if n > 0 && permission.implies(tg::Permission::Read) {
			let namespace = Self::namespace_for_id_with_transaction(transaction, namespace_id)
				.await?
				.ok_or_else(|| tg::error!("failed to find the namespace"))?;
			Self::decrement_namespace_visibility_for_group_with_transaction(
				transaction,
				&namespace,
				group,
			)
			.await?;
		}
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn delete_tag_grant_for_all_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
		name: &tg::tag::Name,
	) -> tg::Result<Option<()>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from tag_grants
				where namespace = {p}1 and name = {p}2 and "all" and permission = 'read';
			"#
		);
		let n = transaction
			.execute(
				statement.into(),
				db::params![namespace_id, name.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if n > 0 {
			let namespace = Self::namespace_for_id_with_transaction(transaction, namespace_id)
				.await?
				.ok_or_else(|| tg::error!("failed to find the namespace"))?;
			Self::decrement_namespace_visibility_for_all_with_transaction(transaction, &namespace)
				.await?;
		}
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
			all: bool,
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
					tag_grants."all" as "all",
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
				all: row.all,
				permission: row.permission,
				created_at: row.created_at,
				created_by: row.created_by,
			})
			.collect())
	}

	pub(crate) async fn get_tag_grant_for_user_with_transaction(
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
			all: false,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	pub(crate) async fn get_tag_grant_for_group_with_transaction(
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
			all: false,
			permission: permission
				.parse()
				.map_err(|error| tg::error!(!error, "invalid permission"))?,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}

	pub(crate) async fn get_tag_grant_for_all_with_transaction(
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
				where namespace = {p}1 and name = {p}2 and "all" and permission = 'read';
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
			all: true,
			permission: tg::Permission::Read,
			created_at: row.created_at,
			created_by: row.created_by,
		})
	}
}
