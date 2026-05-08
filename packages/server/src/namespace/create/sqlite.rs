use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn create_namespace_sqlite(
		&self,
		database: &db::sqlite::Database,
		namespace: &tg::Namespace,
	) -> tg::Result<()> {
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let namespace = namespace.clone();
		connection
			.with(move |connection, _cache| {
				let transaction = connection
					.transaction()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				Self::get_or_create_namespace_sqlite_sync(&transaction, &namespace)?;
				transaction
					.commit()
					.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
				Ok(())
			})
			.await
	}

	pub(crate) fn get_or_create_namespace_sqlite_sync(
		transaction: &sqlite::Transaction,
		namespace: &tg::Namespace,
	) -> tg::Result<i64> {
		if namespace.is_root() {
			return Ok(0);
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
		let mut statement = transaction
			.prepare(&statement)
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params_from_iter(names.iter()))
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut existing = HashMap::<String, i64>::new();
		while let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to get the next row"))?
		{
			let id = row
				.get(0)
				.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
			let name = row
				.get(1)
				.map_err(|error| tg::error!(!error, "failed to get the name column"))?;
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
					on conflict (name) do nothing ;
				"
			);
			transaction
				.execute(statement, sqlite::params![parent, component, name])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			parent = transaction
				.query_row(
					"select id from namespaces where name = ?1;",
					sqlite::params![name],
					|row| row.get(0),
				)
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(parent)
	}
}
