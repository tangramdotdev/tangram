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

pub struct SqliteConnection {
	connection: sqlite::Connection,
}

pub struct PostgresConnection {
	client: postgres::Client,
	statements: tokio::sync::Mutex<HashMap<String, postgres::Statement, fnv::FnvBuildHasher>>,
	task: tokio::task::JoinHandle<()>,
}

impl Database {
	pub async fn new_sqlite(path: PathBuf) -> Result<Self> {
		Ok(Self::Sqlite(Sqlite::new(path).await?))
	}

	pub async fn new_postgres(url: Url, max_connections: usize) -> Result<Self> {
		Ok(Self::Postgres(Postgres::new(url, max_connections).await?))
	}
}

impl Sqlite {
	pub async fn new(path: PathBuf) -> Result<Self> {
		let n = std::thread::available_parallelism().unwrap().get();
		let pool = tangram_util::pool::Pool::new();
		for _ in 0..n {
			let connection = SqliteConnection::connect(&path)?;
			pool.put(connection).await;
		}
		let database = Sqlite { pool };
		Ok(database)
	}

	pub async fn get(&self) -> Result<tangram_util::pool::Guard<'_, SqliteConnection>> {
		let connection = self.pool.get().await;
		Ok(connection)
	}
}

impl Postgres {
	pub async fn new(url: Url, max_connections: usize) -> Result<Self> {
		let pool = tangram_util::pool::Pool::new();
		for _ in 0..max_connections {
			let client = PostgresConnection::connect(&url).await?;
			pool.put(client).await;
		}
		let database = Self { pool, url };
		Ok(database)
	}

	pub async fn get(&self) -> Result<tangram_util::pool::Guard<'_, PostgresConnection>> {
		let mut connection = self.pool.get().await;
		if connection.is_closed() {
			connection.replace(PostgresConnection::connect(&self.url).await?);
		}
		Ok(connection)
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
			if let Err(error) = connection.await {
				tracing::error!(?error);
			}
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
