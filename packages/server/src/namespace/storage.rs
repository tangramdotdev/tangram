use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

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
			r"
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

	pub(crate) async fn namespace_path_has_tag_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<bool> {
		let components = namespace.components().collect::<Vec<_>>();
		let Some((name, namespace_components)) = components.split_last() else {
			return Ok(false);
		};
		let parent = tg::Namespace::with_components(
			namespace_components
				.iter()
				.map(|component| (*component).to_owned()),
		);
		let Some(parent_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &parent).await?
		else {
			return Ok(false);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select 1
				from tags
				where namespace = {p}1 and name = {p}2;
			"
		);
		transaction
			.query_optional(statement.into(), db::params![parent_id, *name])
			.await
			.map(|row| row.is_some())
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	pub(crate) async fn try_get_tag_namespace_id_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<Option<i64>> {
		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace).await?
		else {
			return Ok(None);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select 1
				from tags
				where namespace = {p}1 and name = {p}2;
			"
		);
		transaction
			.query_optional(
				statement.into(),
				db::params![namespace_id, tag.name.to_string()],
			)
			.await
			.map(|row| row.map(|_| namespace_id))
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))
	}

	pub(crate) async fn get_namespace_ancestor_ids_with_transaction(
		transaction: &Transaction<'_>,
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
		ids.extend(
			transaction
				.query_all_value_into::<i64>(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?,
		);
		Ok(ids)
	}

	pub(crate) async fn namespace_for_id_with_transaction(
		transaction: &Transaction<'_>,
		namespace_id: i64,
	) -> tg::Result<Option<tg::Namespace>> {
		if namespace_id == 0 {
			return Ok(Some(tg::Namespace::default()));
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select name
				from namespaces
				where id = {p}1;
			"
		);
		transaction
			.query_optional_value_into::<String>(statement.into(), db::params![namespace_id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.map(|namespace| namespace.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, "invalid namespace"))
	}

	pub(crate) async fn get_or_create_namespace_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<i64> {
		#[cfg(feature = "postgres")]
		if let Transaction::Postgres(transaction) = transaction {
			return Self::get_or_create_namespace_postgres(transaction, namespace).await;
		}

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
				r"
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
				r"
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
				r"
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
}
