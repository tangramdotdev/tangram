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

	pub(crate) async fn increment_namespace_visibility_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		principal: &tg::Principal,
	) -> tg::Result<()> {
		let p = transaction.p();
		let principal = principal.to_string();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r"
					insert into namespace_visibility (namespace, principal, count)
					values ({p}1, {p}2, 1)
					on conflict (namespace, principal)
					do update set count = namespace_visibility.count + 1;
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![namespace_id, principal.clone()],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(())
	}

	pub(crate) async fn decrement_namespace_visibility_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
		principal: &tg::Principal,
	) -> tg::Result<()> {
		let p = transaction.p();
		let principal = principal.to_string();
		for namespace_id in
			Self::namespace_visibility_ids_with_transaction(transaction, namespace).await?
		{
			let statement = formatdoc!(
				r"
					delete from namespace_visibility
					where namespace = {p}1 and principal = {p}2 and count = 1;
				"
			);
			let deleted = transaction
				.execute(
					statement.into(),
					db::params![namespace_id, principal.clone()],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if deleted == 0 {
				let statement = formatdoc!(
					r"
						update namespace_visibility
						set count = count - 1
						where namespace = {p}1 and principal = {p}2 and count > 1;
					"
				);
				transaction
					.execute(
						statement.into(),
						db::params![namespace_id, principal.clone()],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			}
		}
		Ok(())
	}
}
