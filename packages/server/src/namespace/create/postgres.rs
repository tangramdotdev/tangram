use {
	crate::Session,
	indoc::indoc,
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn create_namespace_postgres(
		&self,
		database: &db::postgres::Database,
		namespace: &tg::Namespace,
	) -> tg::Result<()> {
		let mut connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::get_or_create_namespace_postgres(&transaction, namespace).await?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(crate) async fn get_or_create_namespace_postgres(
		transaction: &db::postgres::Transaction<'_>,
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

		let statement = "select id, name from namespaces where name = any($1);";
		let rows = transaction
			.inner()
			.query(statement, &[&names])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
					returning id ;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&parent, component, name])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			parent = rows
				.first()
				.ok_or_else(|| tg::error!("expected a row"))?
				.try_get(0)
				.map_err(|error| tg::error!(!error, "failed to get the id column"))?;
		}

		Ok(parent)
	}
}
