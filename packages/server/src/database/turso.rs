use {
	futures::FutureExt as _,
	std::{ops::ControlFlow, ops::Deref as _, time::Duration},
	tangram_client::prelude::*,
};

pub async fn initialize(
	connection: &::turso::Connection,
	busy_timeout: Duration,
) -> Result<(), tangram_database_turso::Error> {
	connection.busy_timeout(busy_timeout)?;
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

pub async fn create_schema(database: &tangram_database_turso::Database) -> tg::Result<()> {
	database
		.run(|transaction| async move { create_schema_with_transaction(transaction).await }.boxed())
		.await
		.map_err(|error| tg::error!(!error, "failed to create the database schema"))?;
	Ok(())
}

async fn create_schema_with_transaction(
	transaction: &tangram_database_turso::Transaction<'_>,
) -> tg::Result<ControlFlow<(), tangram_database_turso::Error>> {
	let sql = include_str!("./sqlite.sql");
	let result = transaction
		.inner()
		.deref()
		.execute_batch(sql)
		.await
		.map_err(tangram_database_turso::Error::from);
	crate::database::retry!(result, "failed to execute the statements");
	let result = transaction
		.inner()
		.deref()
		.execute_batch(
			"insert into remotes (name, trusted, url) values ('default', 1, 'https://cloud.tangram.dev');",
		)
		.await
		.map_err(tangram_database_turso::Error::from);
	crate::database::retry!(result, "failed to execute the statements");

	Ok(ControlFlow::Break(()))
}
