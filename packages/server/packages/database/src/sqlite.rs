use {num::ToPrimitive as _, rusqlite as sqlite, tangram_client::prelude::*};

pub fn initialize(
	connection: &sqlite::Connection,
	options: &tangram_database_sqlite::ConnectionOptions,
) -> sqlite::Result<()> {
	if !options
		.flags
		.contains(sqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
	{
		connection.pragma_update(None, "auto_vacuum", "incremental")?;
		connection.pragma_update(None, "journal_mode", "wal")?;
		connection.pragma_update(None, "synchronous", "off")?;
	}
	connection.pragma_update(None, "busy_timeout", "5000")?;
	connection.pragma_update(None, "cache_size", "-20000")?;
	connection.pragma_update(None, "foreign_keys", "on")?;
	connection.pragma_update(None, "mmap_size", "2147483648")?;
	connection.pragma_update(None, "recursive_triggers", "on")?;
	connection.pragma_update(None, "temp_store", "memory")?;

	let function = |context: &sqlite::functions::Context| -> sqlite::Result<sqlite::types::Value> {
		let string = context.get::<String>(0)?;
		let delimiter = context.get::<String>(1)?;
		let index = context.get::<i64>(2)? - 1;
		if index < 0 {
			return Ok(sqlite::types::Value::Null);
		}
		let string = string
			.split(&delimiter)
			.nth(index.to_usize().unwrap())
			.map(ToOwned::to_owned)
			.map_or(sqlite::types::Value::Null, sqlite::types::Value::Text);
		Ok(string)
	};
	let flags = sqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
		| sqlite::functions::FunctionFlags::SQLITE_UTF8;
	connection.create_scalar_function("split_part", 3, flags, function)?;

	Ok(())
}

pub async fn create_schema(database: &tangram_database_sqlite::Database) -> tg::Result<()> {
	database
		.run(create_schema_with_transaction)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the database schema"))?;
	Ok(())
}

fn create_schema_with_transaction(
	transaction: &sqlite::Transaction<'_>,
	_cache: &tangram_database_sqlite::Cache,
) -> tg::Result<std::ops::ControlFlow<(), tangram_database_sqlite::Error>> {
	let sql = include_str!("./sqlite.sql");
	let result = transaction
		.execute_batch(sql)
		.map_err(tangram_database_sqlite::Error::from);
	crate::retry!(result, "failed to execute the statements");
	let id = tg::remote::Id::new();
	let sql = format!(
		"insert into remotes (id, name, trusted, url) values ('{id}', 'default', 1, 'https://cloud.tangram.dev');"
	);
	let result = transaction
		.execute_batch(&sql)
		.map_err(tangram_database_sqlite::Error::from);
	crate::retry!(result, "failed to execute the statements");

	Ok(std::ops::ControlFlow::Break(()))
}
