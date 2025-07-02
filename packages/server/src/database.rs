#![allow(dead_code)]

use futures::{Stream, StreamExt as _};
use num::ToPrimitive as _;
use rusqlite as sqlite;
use std::borrow::Cow;
use tangram_client as tg;
use tangram_database::{self as db, Database as _};

#[derive(
	Debug,
	derive_more::IsVariant,
	derive_more::Display,
	derive_more::Error,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Error {
	Sqlite(db::sqlite::Error),
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Database {
	Sqlite(db::sqlite::Database),
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Database),
}

#[allow(clippy::module_name_repetitions)]
#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum DatabaseOptions {
	Sqlite(db::sqlite::DatabaseOptions),
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::DatabaseOptions),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Connection {
	Sqlite(db::pool::Guard<db::sqlite::Connection>),
	#[cfg(feature = "postgres")]
	Postgres(db::pool::Guard<db::postgres::Connection>),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum ConnectionOptions {
	Sqlite(db::sqlite::ConnectionOptions),
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::ConnectionOptions),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Transaction<'a> {
	Sqlite(db::sqlite::Transaction<'a>),
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Transaction<'a>),
}

impl From<db::sqlite::Error> for Error {
	fn from(error: db::sqlite::Error) -> Self {
		Self::Sqlite(error)
	}
}

#[cfg(feature = "postgres")]
impl From<db::postgres::Error> for Error {
	fn from(error: db::postgres::Error) -> Self {
		Self::Postgres(error)
	}
}

impl db::Error for Error {
	fn is_retry(&self) -> bool {
		match self {
			Self::Sqlite(e) => e.is_retry(),
			#[cfg(feature = "postgres")]
			Self::Postgres(e) => e.is_retry(),
			Self::Other(_) => false,
		}
	}

	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl db::Database for Database {
	type Error = Error;

	type Connection = Connection;

	async fn connection_with_options(
		&self,
		options: db::ConnectionOptions,
	) -> Result<Self::Connection, Self::Error> {
		match self {
			Self::Sqlite(s) => {
				let connection = s
					.connection_with_options(options)
					.await
					.map_err(Error::Sqlite)?;
				Ok(Connection::Sqlite(connection))
			},
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let connection = s
					.connection_with_options(options)
					.await
					.map_err(Error::Postgres)?;
				Ok(Connection::Postgres(connection))
			},
		}
	}
}

impl db::Connection for Connection {
	type Error = Error;

	type Transaction<'a> = Transaction<'a>;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		match self {
			Self::Sqlite(s) => {
				let transaction = s.transaction().await.map_err(Error::Sqlite)?;
				Ok(Transaction::Sqlite(transaction))
			},
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let transaction = s.transaction().await.map_err(Error::Postgres)?;
				Ok(Transaction::Postgres(transaction))
			},
		}
	}
}

impl db::Transaction for Transaction<'_> {
	type Error = Error;

	async fn rollback(self) -> Result<(), Self::Error> {
		match self {
			Self::Sqlite(s) => s.rollback().await.map_err(Error::Sqlite),
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.rollback().await.map_err(Error::Postgres),
		}
	}

	async fn commit(self) -> Result<(), Self::Error> {
		match self {
			Self::Sqlite(s) => s.commit().await.map_err(Error::Sqlite),
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.commit().await.map_err(Error::Postgres),
		}
	}
}

impl db::Query for Connection {
	type Error = Error;

	fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(s) => s.p(),
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.p(),
		}
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<u64, Self::Error> {
		match self {
			Self::Sqlite(s) => s.execute(statement, params).await.map_err(Error::Sqlite),
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.execute(statement, params).await.map_err(Error::Postgres),
		}
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		match self {
			Self::Sqlite(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Sqlite)?;
				Ok(stream.map(|result| result.map_err(Error::Sqlite)).boxed())
			},
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Postgres)?;
				Ok(stream.map(|result| result.map_err(Error::Postgres)).boxed())
			},
		}
	}
}

impl db::Query for Transaction<'_> {
	type Error = Error;

	fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(s) => s.p(),
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.p(),
		}
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<u64, Self::Error> {
		match self {
			Self::Sqlite(s) => s.execute(statement, params).await.map_err(Error::Sqlite),
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.execute(statement, params).await.map_err(Error::Postgres),
		}
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		match self {
			Self::Sqlite(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Sqlite)?;
				Ok(stream.map(|result| result.map_err(Error::Sqlite)).boxed())
			},
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Postgres)?;
				Ok(stream.map(|result| result.map_err(Error::Postgres)).boxed())
			},
		}
	}
}

pub async fn migrate(database: &Database) -> tg::Result<()> {
	#[allow(irrefutable_let_patterns)]
	let Database::Sqlite(database) = database else {
		return Ok(());
	};

	let migrations = vec![migration_0000(database)];

	let connection = database
		.connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	let version =
		connection
			.with(|connection| {
				connection
					.pragma_query_value(None, "user_version", |row| {
						Ok(row.get_unwrap::<_, usize>(0))
					})
					.map_err(|source| tg::error!(!source, "failed to get the version"))
			})
			.await?;
	drop(connection);

	// If this path is from a newer version of Tangram, then return an error.
	if version > migrations.len() {
		return Err(tg::error!(
			r"The database has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Run all migrations and update the version.
	let migrations = migrations.into_iter().enumerate().skip(version);
	for (version, migration) in migrations {
		// Run the migration.
		migration.await?;

		// Update the version.
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

async fn migration_0000(database: &db::sqlite::Database) -> tg::Result<()> {
	let sql = include_str!("database/schema.sql");
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
