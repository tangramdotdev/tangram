#![allow(dead_code)]

use futures::FutureExt as _;
use num::ToPrimitive as _;
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_database::{self as db, Database as _};
use tangram_either::Either;

pub type Error = db::either::Error<db::sqlite::Error, db::postgres::Error>;

pub type Database = Either<db::sqlite::Database, db::postgres::Database>;

#[allow(clippy::module_name_repetitions)]
pub type DatabaseOptions = Either<db::sqlite::DatabaseOptions, db::postgres::DatabaseOptions>;

pub type Connection = Either<db::sqlite::Connection, db::postgres::Connection>;

pub type ConnectionOptions = Either<db::sqlite::ConnectionOptions, db::postgres::ConnectionOptions>;

pub type Transaction<'a> = Either<db::sqlite::Transaction<'a>, db::postgres::Transaction<'a>>;

pub async fn migrate(database: &Database) -> tg::Result<()> {
	if database.is_right() {
		return Ok(());
	}

	let migrations = vec![migration_0000(database).boxed()];

	let version = match database {
		Either::Left(database) => {
			let connection = database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
			connection
				.with(|connection| {
					connection
						.pragma_query_value(None, "user_version", |row| {
							Ok(row.get_unwrap::<_, usize>(0))
						})
						.map_err(|source| tg::error!(!source, "failed to get the version"))
				})
				.await?
		},
		Either::Right(_) => {
			unreachable!()
		},
	};

	// If this path is from a newer version of Tangram, then return an error.
	if version > migrations.len() {
		return Err(tg::error!(
			r"The database has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Run all migrations and update the version file.
	let migrations = migrations.into_iter().enumerate().skip(version);
	for (version, migration) in migrations {
		// Run the migration.
		migration.await?;

		// Update the version.
		match database {
			Either::Left(database) => {
				let connection = database
					.write_connection()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
				connection
					.with(move |connection| {
						connection
							.pragma_update(None, "user_version", version + 1)
							.map_err(|source| tg::error!(!source, "failed to get the version"))
					})
					.await?;
			},
			Either::Right(_) => {
				unreachable!()
			},
		}
	}

	Ok(())
}

pub fn initialize(connection: &sqlite::Connection) -> sqlite::Result<()> {
	connection.pragma_update(None, "auto_vaccum", "incremental")?;
	connection.pragma_update(None, "busy_timeout", "5000")?;
	connection.pragma_update(None, "cache_size", "-20000")?;
	connection.pragma_update(None, "foreign_keys", "on")?;
	connection.pragma_update(None, "journal_mode", "wal")?;
	connection.pragma_update(None, "mmap_size", "2147483648")?;
	connection.pragma_update(None, "recursive_triggers", "on")?;
	connection.pragma_update(None, "synchronous", "normal")?;
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

async fn migration_0000(database: &Database) -> tg::Result<()> {
	let sql = include_str!("database/schema.sql");
	let database = database.as_ref().unwrap_left();
	let connection = database
		.write_connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	connection
		.with(move |connection| {
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	connection
		.with(move |connection| {
			let sql =
				"insert into remotes (name, url) values ('default', 'https://cloud.tangram.dev');";
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
}
