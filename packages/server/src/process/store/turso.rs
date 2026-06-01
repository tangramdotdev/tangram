use {
	num::ToPrimitive as _,
	std::ops::Deref as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub async fn migrate(database: &db::turso::Database) -> tg::Result<()> {
	let schema_version = 1;

	let connection = database
		.connection()
		.await
		.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
	let version = connection
		.query_one_value_into::<i64>("pragma user_version".into(), db::params![])
		.await
		.map_err(|error| tg::error!(!error, "failed to get the version"))?
		.to_usize()
		.unwrap();
	drop(connection);

	if version > schema_version {
		return Err(tg::error!(
			r"The process store has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	if version == 0 {
		migration_0000(database)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the process store schema"))?;
	}

	if version < schema_version {
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		connection
			.execute(
				format!("pragma user_version = {schema_version}").into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to set the version"))?;
	}

	Ok(())
}

async fn migration_0000(database: &db::turso::Database) -> tg::Result<()> {
	let sql = include_str!("./sqlite.sql");
	let mut connection = database
		.write_connection()
		.await
		.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
	let transaction = connection
		.transaction()
		.await
		.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
	transaction
		.inner()
		.deref()
		.execute_batch(sql)
		.await
		.map_err(|error| tg::error!(!error, "failed to execute the statements"))?;
	transaction
		.commit()
		.await
		.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
	Ok(())
}
