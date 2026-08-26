use {
	futures::FutureExt as _,
	num::ToPrimitive as _,
	std::{ops::ControlFlow, ops::Deref as _, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub async fn initialize(connection: &::turso::Connection) -> Result<(), db::turso::Error> {
	connection.busy_timeout(Duration::from_secs(5))?;
	for sql in [
		"pragma cache_size = -20000",
		"pragma foreign_keys = on",
		"pragma journal_mode = wal",
		"pragma synchronous = off",
		"pragma temp_store = memory",
	] {
		let mut statement = connection.prepare(sql).await?;
		let mut rows = statement.query(()).await?;
		while rows.next().await?.is_some() {}
	}
	Ok(())
}

pub async fn migrate(database: &db::turso::Database) -> tg::Result<()> {
	let schema_version = 1;

	let version = database
		.run(|transaction| {
			async move { get_database_version_with_transaction(transaction).await }.boxed()
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to get the database version"))?;

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
		database
			.run(|transaction| {
				async move { migration_0000_with_transaction(transaction, schema_version).await }
					.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to create the database schema"))?;
	}

	Ok(())
}

async fn get_database_version_with_transaction(
	transaction: &db::turso::Transaction<'_>,
) -> tg::Result<ControlFlow<usize, db::turso::Error>> {
	let result = transaction
		.query_one_value_into::<i64>("pragma user_version".into(), db::params![])
		.await;
	let version = crate::database::retry!(result, "failed to get the version")
		.to_usize()
		.unwrap();

	Ok(ControlFlow::Break(version))
}

async fn migration_0000_with_transaction(
	transaction: &db::turso::Transaction<'_>,
	schema_version: usize,
) -> tg::Result<ControlFlow<(), db::turso::Error>> {
	let sql = include_str!("./sqlite.sql");
	let result = transaction
		.inner()
		.deref()
		.execute_batch(sql)
		.await
		.map_err(db::turso::Error::from);
	crate::database::retry!(result, "failed to execute the statements");
	let result = transaction
		.inner()
		.deref()
		.execute_batch(
			"insert into remotes (name, trusted, url) values ('default', 1, 'https://cloud.tangram.dev');",
		)
		.await
		.map_err(db::turso::Error::from);
	crate::database::retry!(result, "failed to execute the statements");
	let result = transaction
		.execute(
			format!("pragma user_version = {schema_version}").into(),
			db::params![],
		)
		.await;
	crate::database::retry!(result, "failed to set the version");

	Ok(ControlFlow::Break(()))
}
