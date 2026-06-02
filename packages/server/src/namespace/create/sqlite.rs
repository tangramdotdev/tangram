use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::{collections::HashMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) fn get_or_create_namespace_sqlite_sync(
		transaction: &sqlite::Transaction,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<i64, db::sqlite::Error>> {
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

		let placeholders = vec!["?"; names.len()].join(", ");
		let statement = format!("select id, name from namespaces where name in ({placeholders});");
		let result = transaction
			.prepare(&statement)
			.map_err(db::sqlite::Error::from);
		let mut statement = crate::database::retry!(result, "failed to prepare the statement");
		let mut rows = {
			let result = statement
				.query(sqlite::params_from_iter(names.iter()))
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement")
		};
		let mut existing = HashMap::<String, i64>::new();
		loop {
			let result = rows.next().map_err(db::sqlite::Error::from);
			let Some(row) = crate::database::retry!(result, "failed to get the next row") else {
				break;
			};
			let result = row.get(0).map_err(db::sqlite::Error::from);
			let id = crate::database::retry!(result, "failed to get the id column");
			let result = row.get(1).map_err(db::sqlite::Error::from);
			let name = crate::database::retry!(result, "failed to get the name column");
			existing.insert(name, id);
		}
		drop(rows);
		drop(statement);

		let mut parent = 0;
		for (component, name) in components.iter().zip(&names) {
			if let Some(id) = existing.get(name).copied() {
				parent = id;
				continue;
			}
			let statement = indoc!(
				"
					insert into namespaces (parent, component, name)
					values (?1, ?2, ?3)
					on conflict (name) do nothing;
				"
			);
			let result = transaction
				.execute(statement, sqlite::params![parent, component, name])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
			let result = transaction
				.query_row(
					"select id from namespaces where name = ?1;",
					sqlite::params![name],
					|row| row.get(0),
				)
				.map_err(db::sqlite::Error::from);
			parent = crate::database::retry!(result, "failed to execute the statement");
		}
		Ok(ControlFlow::Break(parent))
	}
}
