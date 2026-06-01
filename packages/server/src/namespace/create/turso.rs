use {
	crate::Session,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn get_or_create_namespace_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<i64, db::turso::Error>> {
		if namespace.is_root() {
			return Ok(ControlFlow::Break(0));
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
			let result = transaction
				.query_optional_value_into::<i64>(statement.into(), params)
				.await;
			if let Some(id) = crate::database::retry!(result, "failed to execute the statement") {
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
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to execute the statement");

			let statement = formatdoc!(
				r"
					select id
					from namespaces
					where name = {p}1;
				"
			);
			let params = db::params![name.clone()];
			let result = transaction
				.query_one_value_into::<i64>(statement.into(), params)
				.await;
			parent = crate::database::retry!(result, "failed to execute the statement");
		}

		Ok(ControlFlow::Break(parent))
	}
}
