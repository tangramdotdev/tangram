use {
	num::ToPrimitive as _,
	std::{ops::Deref as _, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub async fn initialize(connection: &::turso::Connection) -> Result<(), db::turso::Error> {
	connection.busy_timeout(Duration::from_secs(5))?;
	let transaction = turso::transaction::Transaction::new_unchecked(
		connection,
		turso::transaction::TransactionBehavior::Deferred,
	)
	.await?;
	for sql in [
		"pragma cache_size = -20000",
		"pragma foreign_keys = on",
		"pragma journal_mode = wal",
		"pragma synchronous = off",
		"pragma temp_store = memory",
	] {
		let mut statement = transaction.prepare(sql).await?;
		let mut rows = statement.query(()).await?;
		while rows.next().await?.is_some() {}
	}
	transaction.commit().await?;
	Ok(())
}

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
			r"The database has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	if version != 0 && version != schema_version {
		return Err(tg::error!(
			"the database schema is incompatible with this version of tangram; please recreate the data directory"
		));
	}

	if version == 0 {
		migration_0000(database)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the database schema"))?;
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
		.inner()
		.deref()
		.execute_batch(
			"insert into remotes (name, url) values ('default', 'https://cloud.tangram.dev');",
		)
		.await
		.map_err(|error| tg::error!(!error, "failed to execute the statements"))?;
	transaction
		.commit()
		.await
		.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
	Ok(())
}
