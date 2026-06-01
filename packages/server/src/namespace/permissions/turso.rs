use {
	crate::Session,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn increment_namespace_visibility_for_user_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
		user: &tg::user::Id,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
		Self::increment_namespace_visibility_turso_with_transaction(
			transaction,
			namespace,
			&tg::Principal::User(user.clone()),
		)
		.await
	}

	pub(crate) async fn increment_namespace_visibility_for_all_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
		Self::increment_namespace_visibility_turso_with_transaction(
			transaction,
			namespace,
			&tg::Principal::All,
		)
		.await
	}

	pub(crate) async fn decrement_namespace_visibility_for_grant_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
		user: Option<&str>,
		group: Option<&str>,
		all: bool,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
		let namespace_ids = match namespace_visibility_ids_turso(transaction, namespace).await? {
			ControlFlow::Break(namespace_ids) => namespace_ids,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for namespace_id in namespace_ids {
			let principal = if let Some(user) = user {
				user
			} else if let Some(group) = group {
				group
			} else if all {
				"all"
			} else {
				continue;
			};
			match decrement_namespace_visibility_for_principal_turso_with_transaction(
				transaction,
				namespace_id,
				principal,
			)
			.await?
			{
				ControlFlow::Break(()) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		Ok(ControlFlow::Break(()))
	}

	async fn increment_namespace_visibility_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
		let p = transaction.p();
		let principal = principal.to_string();
		let namespace_ids = match namespace_visibility_ids_turso(transaction, namespace).await? {
			ControlFlow::Break(namespace_ids) => namespace_ids,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		for namespace_id in namespace_ids {
			let statement = formatdoc!(
				r"
					insert into namespace_visibility (namespace, principal, count)
					values ({p}1, {p}2, 1)
					on conflict (namespace, principal)
					do update set count = namespace_visibility.count + 1;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![namespace_id, principal.clone()],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
		}
		Ok(ControlFlow::Break(()))
	}
}

async fn namespace_visibility_ids_turso(
	transaction: &db::turso::Transaction<'_>,
	namespace: &tg::Namespace,
) -> tg::Result<ControlFlow<Vec<i64>, db::turso::Error>> {
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
		return Ok(ControlFlow::Break(Vec::new()));
	}
	let p = transaction.p();
	let placeholders = (1..=names.len())
		.map(|i| format!("{p}{i}"))
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
	let result = transaction
		.query_all_value_into::<i64>(statement.into(), params)
		.await;
	ids.extend(crate::database::retry!(
		result,
		"failed to execute the statement"
	));
	Ok(ControlFlow::Break(
		ids.into_iter().filter(|id| *id != 0).collect(),
	))
}

async fn decrement_namespace_visibility_for_principal_turso_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	namespace_id: i64,
	principal: &str,
) -> tg::Result<ControlFlow<(), db::turso::Error>> {
	let result = transaction
		.execute(
			"delete from namespace_visibility where namespace = ?1 and principal = ?2 and count = 1;"
				.into(),
			db::params![namespace_id, principal],
		)
		.await;
	let deleted = crate::database::retry!(result, "failed to execute the statement");
	if deleted == 0 {
		let result = transaction
			.execute(
				"update namespace_visibility set count = count - 1 where namespace = ?1 and principal = ?2 and count > 1;"
					.into(),
				db::params![namespace_id, principal],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");
	}
	Ok(ControlFlow::Break(()))
}
