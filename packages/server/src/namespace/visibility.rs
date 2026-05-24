use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn namespace_visibility_ids_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<i64>> {
		Ok(
			Self::get_namespace_ancestor_ids_with_transaction(transaction, namespace)
				.await?
				.into_iter()
				.filter(|id| *id != 0)
				.collect(),
		)
	}

	pub(crate) async fn increment_namespace_visibility_for_user_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		user: &tg::user::Id,
	) -> tg::Result<()> {
		let p = transaction.p();
		let user = user.to_string();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					insert into namespace_visibility (namespace, "user", count)
					values ({p}1, {p}2, 1)
					on conflict (namespace, "user") where "user" is not null
					do update set count = namespace_visibility.count + 1;
				"#
			);
			transaction
				.execute(statement.into(), db::params![namespace_id, user.clone()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(())
	}

	pub(crate) async fn increment_namespace_visibility_for_group_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		group: &tg::group::Id,
	) -> tg::Result<()> {
		let p = transaction.p();
		let group = group.to_string();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					insert into namespace_visibility (namespace, "group", count)
					values ({p}1, {p}2, 1)
					on conflict (namespace, "group") where "group" is not null
					do update set count = namespace_visibility.count + 1;
				"#
			);
			transaction
				.execute(statement.into(), db::params![namespace_id, group.clone()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(())
	}

	pub(crate) async fn increment_namespace_visibility_for_all_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<()> {
		let p = transaction.p();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					insert into namespace_visibility (namespace, "all", count)
					values ({p}1, true, 1)
					on conflict (namespace) where "all"
					do update set count = namespace_visibility.count + 1;
				"#
			);
			transaction
				.execute(statement.into(), db::params![namespace_id])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(())
	}

	pub(crate) async fn decrement_namespace_visibility_for_user_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		user: &tg::user::Id,
	) -> tg::Result<()> {
		let p = transaction.p();
		let user = user.to_string();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					delete from namespace_visibility
					where namespace = {p}1 and "user" = {p}2 and count = 1;
				"#
			);
			let deleted = transaction
				.execute(statement.into(), db::params![namespace_id, user.clone()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if deleted == 0 {
				let statement = formatdoc!(
					r#"
						update namespace_visibility
						set count = count - 1
						where namespace = {p}1 and "user" = {p}2 and count > 1;
					"#
				);
				transaction
					.execute(statement.into(), db::params![namespace_id, user.clone()])
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			}
		}
		Ok(())
	}

	pub(crate) async fn decrement_namespace_visibility_for_group_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		group: &tg::group::Id,
	) -> tg::Result<()> {
		let p = transaction.p();
		let group = group.to_string();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					delete from namespace_visibility
					where namespace = {p}1 and "group" = {p}2 and count = 1;
				"#
			);
			let deleted = transaction
				.execute(statement.into(), db::params![namespace_id, group.clone()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if deleted == 0 {
				let statement = formatdoc!(
					r#"
						update namespace_visibility
						set count = count - 1
						where namespace = {p}1 and "group" = {p}2 and count > 1;
					"#
				);
				transaction
					.execute(statement.into(), db::params![namespace_id, group.clone()])
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			}
		}
		Ok(())
	}

	pub(crate) async fn decrement_namespace_visibility_for_all_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<()> {
		let p = transaction.p();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r#"
					delete from namespace_visibility
					where namespace = {p}1 and "all" and count = 1;
				"#
			);
			let deleted = transaction
				.execute(statement.into(), db::params![namespace_id])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if deleted == 0 {
				let statement = formatdoc!(
					r#"
						update namespace_visibility
						set count = count - 1
						where namespace = {p}1 and "all" and count > 1;
					"#
				);
				transaction
					.execute(statement.into(), db::params![namespace_id])
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			}
		}
		Ok(())
	}
}
