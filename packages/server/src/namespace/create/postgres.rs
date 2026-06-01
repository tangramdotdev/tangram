use {
	crate::Session,
	indoc::indoc,
	std::{collections::HashMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn get_or_create_namespace_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<i64, db::postgres::Error>> {
		if namespace.is_root() {
			return Ok(ControlFlow::Break(0));
		}

		let mut components = Vec::new();
		let mut names = Vec::new();
		let mut name = String::new();
		for component in namespace.components() {
			if !name.is_empty() {
				name.push('/');
			}
			name.push_str(component);
			components.push(component.to_owned());
			names.push(name.clone());
		}

		let statement = "select id, name from namespaces where name = any($1);";
		let result = transaction
			.inner()
			.query(statement, &[&names])
			.await
			.map_err(db::postgres::Error::from);
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let mut existing = HashMap::<String, i64>::new();
		for row in rows {
			let id = row
				.try_get(0)
				.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
			let name = row
				.try_get(1)
				.map_err(|error| tg::error!(!error, "failed to get the name column"))?;
			existing.insert(name, id);
		}

		let mut parent = 0;
		for (component, name) in components.iter().zip(&names) {
			if let Some(id) = existing.get(name).copied() {
				parent = id;
				continue;
			}
			let statement = indoc!(
				"
					insert into namespaces (parent, component, name)
					values ($1, $2, $3)
					on conflict (name) do update
					set name = excluded.name
					returning id;
				"
			);
			let result = transaction
				.inner()
				.query(statement, &[&parent, component, name])
				.await
				.map_err(db::postgres::Error::from);
			let rows = crate::database::retry!(result, "failed to execute the statement");
			parent = rows
				.first()
				.ok_or_else(|| tg::error!("expected a row"))?
				.try_get(0)
				.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
		}

		Ok(ControlFlow::Break(parent))
	}
}
