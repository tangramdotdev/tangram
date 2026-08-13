use {
	num::ToPrimitive as _, rusqlite as sqlite, tangram_client::prelude::*, tangram_database as db,
};

pub fn initialize(
	connection: &sqlite::Connection,
	options: &db::sqlite::ConnectionOptions,
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

pub async fn migrate(database: &db::sqlite::Database) -> tg::Result<()> {
	let schema_version = 1;

	let version = database
		.run(get_database_version_with_transaction)
		.await
		.map_err(|error| tg::error!(!error, "failed to get the database version"))?;

	if version > schema_version {
		return Err(tg::error!(
			r"The database has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Fail on databases from older incompatible schemas.
	if version != 0 && version != schema_version {
		return Err(tg::error!(
			"the database schema is incompatible with this version of tangram; please recreate the data directory"
		));
	}

	if version == 0 {
		database
			.run(move |transaction, _cache| {
				migration_0000_with_transaction(transaction, schema_version)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to create the database schema"))?;
	}

	Ok(())
}

fn get_database_version_with_transaction(
	transaction: &sqlite::Transaction<'_>,
	_cache: &db::sqlite::Cache,
) -> tg::Result<std::ops::ControlFlow<usize, db::sqlite::Error>> {
	let result = transaction
		.pragma_query_value(None, "user_version", |row| {
			Ok(row.get_unwrap::<_, i64>(0).to_usize().unwrap())
		})
		.map_err(db::sqlite::Error::from);
	let version = crate::database::retry!(result, "failed to get the version");

	Ok(std::ops::ControlFlow::Break(version))
}

fn migration_0000_with_transaction(
	transaction: &sqlite::Transaction<'_>,
	schema_version: usize,
) -> tg::Result<std::ops::ControlFlow<(), db::sqlite::Error>> {
	let sql = include_str!("./sqlite.sql");
	let result = transaction
		.execute_batch(sql)
		.map_err(db::sqlite::Error::from);
	crate::database::retry!(result, "failed to execute the statements");
	let sql = "insert into remotes (name, url) values ('default', 'https://cloud.tangram.dev');";
	let result = transaction
		.execute_batch(sql)
		.map_err(db::sqlite::Error::from);
	crate::database::retry!(result, "failed to execute the statements");
	let result = transaction
		.pragma_update(None, "user_version", schema_version.to_i64().unwrap())
		.map_err(db::sqlite::Error::from);
	crate::database::retry!(result, "failed to set the version");

	Ok(std::ops::ControlFlow::Break(()))
}
