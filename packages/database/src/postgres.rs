use super::{Row, Value};
use futures::{future, Stream, TryStreamExt};
use indexmap::IndexMap;
use itertools::Itertools;
pub use postgres::types::Json;
use std::{borrow::Cow, collections::HashMap, future::Future, pin::pin};
use tangram_error::{error, Result};
use tokio_postgres as postgres;
use url::Url;

pub struct Database {
	pool: tangram_pool::Pool<Connection>,
	url: Url,
}

pub struct Options {
	pub url: Url,
	pub max_connections: usize,
}

pub struct Connection {
	client: postgres::Client,
	cache: Cache,
}

pub struct Transaction<'a> {
	transaction: postgres::Transaction<'a>,
	cache: &'a Cache,
}

#[derive(Default)]
pub struct Cache {
	statements: tokio::sync::Mutex<HashMap<String, postgres::Statement, fnv::FnvBuildHasher>>,
}

impl Database {
	pub async fn new(options: Options) -> Result<Self> {
		let mut connections = Vec::with_capacity(options.max_connections);
		for _ in 0..options.max_connections {
			let client = Connection::connect(&options.url).await?;
			connections.push(client);
		}
		let pool = tangram_pool::Pool::new(connections);
		let database = Self {
			pool,
			url: options.url,
		};
		Ok(database)
	}

	pub async fn connection(&self) -> Result<tangram_pool::Guard<Connection>> {
		let mut connection = self.pool.get().await;
		if connection.client.is_closed() {
			connection.replace(Connection::connect(&self.url).await?);
		}
		Ok(connection)
	}
}

impl Connection {
	pub async fn connect(url: &Url) -> Result<Self> {
		let (client, connection) = postgres::connect(url.as_str(), postgres::NoTls)
			.await
			.map_err(|error| error!(source = error, "failed to connect to the database"))?;
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					tracing::error!(?error);
				})
				.ok();
		});
		let cache = Cache::default();
		let client = Self { client, cache };
		Ok(client)
	}

	pub async fn execute(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<u64> {
		let statement = statement.into();
		let statement = &self.cache.get(&self.client, statement).await?;
		let params = params.into_iter().collect_vec();
		let params = &params
			.iter()
			.map(|value| value as &(dyn postgres::types::ToSql + Sync))
			.collect_vec();
		let n = self
			.client
			.execute(statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;
		Ok(n)
	}

	pub async fn query(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Row>>> {
		let statement = statement.into();
		let statement = &self.cache.get(&self.client, statement).await?;
		let params = params.into_iter().collect_vec();
		let rows = self
			.client
			.query_raw(statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?
			.map_err(|error| error!(source = error, "failed to get a row"));
		let rows = rows.and_then(|row| {
			let mut entries = IndexMap::with_capacity(row.columns().len());
			for (i, column) in row.columns().iter().enumerate() {
				let name = column.name().to_owned();
				let value = row.get::<_, Value>(i);
				entries.insert(name, value);
			}
			let row = Row::with_entries(entries);
			future::ready(Ok(row))
		});
		Ok(rows)
	}

	pub async fn query_optional(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Row>> {
		let rows = self.query(statement, params).await?;
		let row = pin!(rows).try_next().await?;
		Ok(row)
	}

	pub async fn query_one(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Row> {
		let rows = self.query(statement, params).await?;
		let row = pin!(rows)
			.try_next()
			.await?
			.ok_or_else(|| error!("expected a row"))?;
		Ok(row)
	}

	pub async fn query_all(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Row>> {
		let rows = self.query(statement, params).await?;
		let rows = rows.try_collect().await?;
		Ok(rows)
	}

	pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
		let transaction = self
			.client
			.transaction()
			.await
			.map_err(|error| error!(source = error, "failed to begin the transaction"))?;
		Ok(Transaction {
			transaction,
			cache: &self.cache,
		})
	}

	pub async fn with<F, Fut, R>(&mut self, f: F) -> Result<R>
	where
		F: FnOnce(&mut postgres::Client, &Cache) -> Fut,
		Fut: Future<Output = Result<R>>,
	{
		f(&mut self.client, &self.cache).await
	}
}

impl<'a> Transaction<'a> {
	pub async fn execute(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<u64> {
		let statement = statement.into();
		let statement = &self.cache.get(&self.transaction, statement).await?;
		let params = params.into_iter().collect_vec();
		let params = &params
			.iter()
			.map(|value| value as &(dyn postgres::types::ToSql + Sync))
			.collect_vec();
		let n = self
			.transaction
			.execute(statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?;
		Ok(n)
	}

	pub async fn query(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Row>>> {
		let statement = statement.into();
		let statement = &self.cache.get(&self.transaction, statement).await?;
		let params = params.into_iter().collect_vec();
		let rows = self
			.transaction
			.query_raw(statement, params)
			.await
			.map_err(|error| error!(source = error, "failed to execute the statement"))?
			.map_err(|error| error!(source = error, "failed to get a row"));
		let rows = rows.and_then(|row| {
			let mut entries = IndexMap::with_capacity(row.columns().len());
			for (i, column) in row.columns().iter().enumerate() {
				let name = column.name().to_owned();
				let value = row.get::<_, Value>(i);
				entries.insert(name, value);
			}
			let row = Row::with_entries(entries);
			future::ready(Ok(row))
		});
		Ok(rows)
	}

	pub async fn query_optional(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Row>> {
		let rows = self.query(statement, params).await?;
		let row = pin!(rows).try_next().await?;
		Ok(row)
	}

	pub async fn query_one(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Row> {
		let rows = self.query(statement, params).await?;
		let row = pin!(rows)
			.try_next()
			.await?
			.ok_or_else(|| error!("expected a row"))?;
		Ok(row)
	}

	pub async fn query_all(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Row>> {
		let rows = self.query(statement, params).await?;
		let rows = rows.try_collect().await?;
		Ok(rows)
	}

	pub async fn rollback(self) -> Result<()> {
		self.transaction
			.rollback()
			.await
			.map_err(|error| error!(source = error, "failed to roll back the transaction"))
	}

	pub async fn commit(self) -> Result<()> {
		self.transaction
			.commit()
			.await
			.map_err(|error| error!(source = error, "failed to commit the transaction"))
	}

	pub async fn with<F, Fut, R>(&mut self, f: F) -> Result<R>
	where
		F: FnOnce(&mut postgres::Transaction, &Cache) -> Fut,
		Fut: Future<Output = Result<R>>,
	{
		f(&mut self.transaction, self.cache).await
	}
}

impl Cache {
	pub async fn get(
		&self,
		client: &impl postgres::GenericClient,
		query: impl AsRef<str>,
	) -> Result<postgres::Statement> {
		if let Some(statement) = self.statements.lock().await.get(query.as_ref()) {
			return Ok(statement.clone());
		}
		let statement = client
			.prepare(query.as_ref())
			.await
			.map_err(|error| error!(source = error, "failed to prepare the statement"))?;
		self.statements
			.lock()
			.await
			.insert(query.as_ref().to_owned(), statement.clone());
		Ok(statement)
	}
}

impl postgres::types::ToSql for Value {
	fn to_sql(
		&self,
		ty: &postgres::types::Type,
		out: &mut bytes::BytesMut,
	) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
	where
		Self: Sized,
	{
		match self {
			Value::Null => Ok(postgres::types::IsNull::Yes),
			Value::Integer(value) => value.to_sql(ty, out),
			Value::Real(value) => value.to_sql(ty, out),
			Value::Text(value) => value.to_sql(ty, out),
			Value::Blob(value) => value.to_sql(ty, out),
		}
	}

	postgres::types::accepts!(BOOL, INT8, FLOAT8, TEXT, BYTEA);

	postgres::types::to_sql_checked!();
}

impl<'a> postgres::types::FromSql<'a> for Value {
	fn from_sql(
		ty: &postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		match *ty {
			postgres::types::Type::BOOL => Ok(Self::Integer(bool::from_sql(ty, raw)?.into())),
			postgres::types::Type::INT8 | postgres::types::Type::NUMERIC => {
				Ok(Self::Integer(i64::from_sql(ty, raw)?))
			},
			postgres::types::Type::FLOAT8 => Ok(Self::Real(f64::from_sql(ty, raw)?)),
			postgres::types::Type::TEXT => Ok(Self::Text(String::from_sql(ty, raw)?)),
			postgres::types::Type::BYTEA => Ok(Self::Blob(<Vec<u8>>::from_sql(ty, raw)?)),
			_ => Err(Box::new(error!("invalid type"))),
		}
	}

	fn from_sql_null(
		_: &postgres::types::Type,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(Self::Null)
	}

	postgres::types::accepts!(BOOL, INT8, NUMERIC, FLOAT8, TEXT, BYTEA);
}

#[allow(clippy::module_name_repetitions)]
#[macro_export]
macro_rules! postgres_params {
	($($x:expr),* $(,)?) => {
		&[$(&$x as &(dyn tokio_postgres::types::ToSql + Sync),)*] as &[&(dyn tokio_postgres::types::ToSql + Sync)]
	};
}

pub use postgres_params as params;
