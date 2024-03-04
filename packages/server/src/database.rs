use itertools::Itertools;
use rusqlite as sqlite;
use std::{
	collections::HashMap,
	path::{Path, PathBuf},
};
use tangram_error::{Result, WrapErr};
use tokio_postgres as postgres;
pub use tokio_postgres::types::Json as PostgresJson;
use url::Url;

pub enum Database {
	Sqlite(Sqlite),
	Postgres(Postgres),
}

pub struct Sqlite {
	pool: tangram_util::pool::Pool<SqliteConnection>,
}

pub struct Postgres {
	pool: tangram_util::pool::Pool<PostgresConnection>,
	url: Url,
}

pub enum Connection {
	Sqlite(tangram_util::pool::Guard<SqliteConnection>),
	Postgres(tangram_util::pool::Guard<PostgresConnection>),
}

pub struct SqliteConnection {
	connection: sqlite::Connection,
}

pub struct PostgresConnection {
	client: postgres::Client,
	statements: tokio::sync::Mutex<HashMap<String, postgres::Statement, fnv::FnvBuildHasher>>,
	task: tokio::task::JoinHandle<()>,
}

pub enum Transaction<'a> {
	Sqlite(rusqlite::Transaction<'a>),
	Postgres(PostgresTransaction<'a>),
}

pub struct PostgresTransaction<'a> {
	statements: &'a tokio::sync::Mutex<HashMap<String, postgres::Statement, fnv::FnvBuildHasher>>,
	transaction: postgres::Transaction<'a>,
}

impl Database {
	pub async fn new_sqlite(path: PathBuf, max_connections: usize) -> Result<Self> {
		Ok(Self::Sqlite(Sqlite::new(path, max_connections).await?))
	}

	pub async fn new_postgres(url: Url, max_connections: usize) -> Result<Self> {
		Ok(Self::Postgres(Postgres::new(url, max_connections).await?))
	}

	pub async fn get(&self) -> Result<Connection> {
		match self {
			Database::Sqlite(database) => Ok(Connection::Sqlite(database.get().await?)),
			Database::Postgres(database) => Ok(Connection::Postgres(database.get().await?)),
		}
	}
}

impl Sqlite {
	pub async fn new(path: PathBuf, max_connections: usize) -> Result<Self> {
		let connections = (0..max_connections)
			.map(|_| SqliteConnection::connect(&path))
			.try_collect()?;
		let pool = tangram_util::pool::Pool::new(connections);
		let database = Sqlite { pool };
		Ok(database)
	}

	pub async fn get(&self) -> Result<tangram_util::pool::Guard<SqliteConnection>> {
		let connection = self.pool.get().await;
		Ok(connection)
	}
}

impl Postgres {
	pub async fn new(url: Url, max_connections: usize) -> Result<Self> {
		let mut connections = Vec::with_capacity(max_connections);
		for _ in 0..max_connections {
			let client = PostgresConnection::connect(&url).await?;
			connections.push(client);
		}
		let pool = tangram_util::pool::Pool::new(connections);
		let database = Self { pool, url };
		Ok(database)
	}

	pub async fn get(&self) -> Result<tangram_util::pool::Guard<PostgresConnection>> {
		let mut connection = self.pool.get().await;
		if connection.is_closed() {
			connection.replace(PostgresConnection::connect(&self.url).await?);
		}
		Ok(connection)
	}
}

impl Connection {
	pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
		match self {
			Self::Sqlite(database) => Ok(Transaction::Sqlite(
				database
					.transaction()
					.wrap_err("Failed to get sqlite transaction.")?,
			)),
			Self::Postgres(database) => Ok(Transaction::Postgres(
				database
					.transaction()
					.await
					.wrap_err("Failed to get postgres transaction.")?,
			)),
		}
	}
}

impl SqliteConnection {
	pub fn connect(path: &Path) -> Result<Self> {
		let connection = sqlite::Connection::open(path).wrap_err("Failed to open the database.")?;
		connection
			.pragma_update(None, "busy_timeout", "86400000")
			.wrap_err("Failed to set the busy timeout.")?;
		Ok(Self { connection })
	}
}

impl PostgresConnection {
	pub async fn connect(url: &Url) -> Result<Self> {
		let (client, connection) = postgres::connect(url.as_str(), postgres::NoTls)
			.await
			.wrap_err("Failed to connect to the database.")?;
		let task = tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					tracing::error!(?error);
				})
				.ok();
		});
		let statements = tokio::sync::Mutex::new(HashMap::default());
		let client = Self {
			client,
			statements,
			task,
		};
		Ok(client)
	}

	#[allow(dead_code)]
	pub async fn join(self) -> Result<()> {
		drop(self.client);
		self.task.await.wrap_err("Failed to join the task.")?;
		Ok(())
	}

	pub async fn prepare_cached(
		&self,
		query: impl Into<String>,
	) -> Result<postgres::Statement, postgres::Error> {
		let query = query.into();
		if let Some(statement) = self.statements.lock().await.get(&query) {
			return Ok(statement.clone());
		}
		let statement = self.client.prepare(&query).await?;
		self.statements
			.lock()
			.await
			.insert(query, statement.clone());
		Ok(statement)
	}

	pub async fn transaction(&mut self) -> Result<PostgresTransaction<'_>, postgres::Error> {
		let transaction = self.client.transaction().await?;
		Ok(PostgresTransaction {
			statements: &self.statements,
			transaction,
		})
	}
}

impl<'a> Transaction<'a> {
	pub async fn commit(self) -> Result<()> {
		match self {
			Self::Sqlite(txn) => txn
				.commit()
				.wrap_err("Failed to commit sqlite transaction."),
			Self::Postgres(txn) => txn
				.commit()
				.await
				.wrap_err("Failed to commit postgres transaction."),
		}
	}
}

impl<'a> PostgresTransaction<'a> {
	pub async fn prepare_cached(
		&self,
		query: impl Into<String>,
	) -> Result<postgres::Statement, postgres::Error> {
		let query = query.into();
		if let Some(statement) = self.statements.lock().await.get(&query) {
			return Ok(statement.clone());
		}
		let statement = self.transaction.prepare(&query).await?;
		self.statements
			.lock()
			.await
			.insert(query, statement.clone());
		Ok(statement)
	}

	pub async fn commit(self) -> Result<(), postgres::Error> {
		self.transaction.commit().await
	}
}

impl std::ops::Deref for SqliteConnection {
	type Target = sqlite::Connection;

	fn deref(&self) -> &Self::Target {
		&self.connection
	}
}

impl std::ops::DerefMut for SqliteConnection {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.connection
	}
}

impl std::ops::Deref for PostgresConnection {
	type Target = postgres::Client;

	fn deref(&self) -> &Self::Target {
		&self.client
	}
}

impl std::ops::DerefMut for PostgresConnection {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.client
	}
}

impl<'a> std::ops::Deref for PostgresTransaction<'a> {
	type Target = postgres::Transaction<'a>;

	fn deref(&self) -> &Self::Target {
		&self.transaction
	}
}

impl<'a> std::ops::DerefMut for PostgresTransaction<'a> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.transaction
	}
}

pub struct SqliteJson<T>(pub T);

impl<T> sqlite::types::ToSql for SqliteJson<T>
where
	T: serde::Serialize,
{
	fn to_sql(&self) -> sqlite::Result<sqlite::types::ToSqlOutput<'_>> {
		let json = serde_json::to_string(&self.0)
			.map_err(|error| sqlite::Error::ToSqlConversionFailure(error.into()))?;
		Ok(sqlite::types::ToSqlOutput::Owned(
			sqlite::types::Value::Text(json),
		))
	}
}

impl<T> sqlite::types::FromSql for SqliteJson<T>
where
	T: serde::de::DeserializeOwned,
{
	fn column_result(value: sqlite::types::ValueRef<'_>) -> sqlite::types::FromSqlResult<Self> {
		let json = value.as_str()?;
		let value = serde_json::from_str(json)
			.map_err(|error| sqlite::types::FromSqlError::Other(error.into()))?;
		Ok(Self(value))
	}
}

#[macro_export]
macro_rules! sqlite_params {
	($($x:expr),* $(,)?) => {
		&[$(&$x as &(dyn rusqlite::types::ToSql),)*] as &[&(dyn rusqlite::types::ToSql)]
	};
}

#[macro_export]
macro_rules! postgres_params {
	($($x:expr),* $(,)?) => {
		&[$(&$x as &(dyn tokio_postgres::types::ToSql + Sync),)*] as &[&(dyn tokio_postgres::types::ToSql + Sync)]
	};
}
