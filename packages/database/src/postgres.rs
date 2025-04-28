use crate::{
	Row, Value,
	pool::{self, Pool},
};
use futures::{Stream, TryStreamExt as _, future};
use indexmap::IndexMap;
use itertools::Itertools as _;
use std::{borrow::Cow, collections::HashMap};
use tokio_postgres as postgres;
use url::Url;

pub use postgres::types::Json;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Postgres(postgres::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug)]
pub struct DatabaseOptions {
	pub url: Url,
	pub connections: usize,
}

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
	pub url: Url,
}

#[derive(Default)]
pub struct Cache {
	statements:
		tokio::sync::Mutex<HashMap<Cow<'static, str>, postgres::Statement, fnv::FnvBuildHasher>>,
}

pub struct Database {
	pool: Pool<Connection>,
}

pub struct Connection {
	options: ConnectionOptions,
	client: postgres::Client,
	cache: Cache,
}

pub struct Transaction<'a> {
	transaction: postgres::Transaction<'a>,
	cache: &'a Cache,
}

impl Cache {
	pub async fn get(
		&self,
		client: &impl postgres::GenericClient,
		statement: Cow<'static, str>,
	) -> Result<postgres::Statement, Error> {
		let key = statement;
		if let Some(statement) = self.statements.lock().await.get(&key) {
			return Ok(statement.clone());
		}
		let statement = client.prepare(&key).await?;
		self.statements.lock().await.insert(key, statement.clone());
		Ok(statement)
	}
}

impl Database {
	pub async fn new(options: DatabaseOptions) -> Result<Self, Error> {
		let pool = Pool::new();
		for _ in 0..options.connections {
			let options = ConnectionOptions {
				url: options.url.clone(),
			};
			let connection = Connection::connect(options).await?;
			pool.add(connection);
		}
		let database = Self { pool };
		Ok(database)
	}

	#[must_use]
	pub fn pool(&self) -> &Pool<Connection> {
		&self.pool
	}
}

impl Connection {
	pub async fn connect(options: ConnectionOptions) -> Result<Self, Error> {
		let (client, connection) = postgres::connect(options.url.as_str(), postgres::NoTls).await?;
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| tracing::error!(?error, "postgres connection failed"))
				.ok();
		});
		let cache = Cache::default();
		let connection = Self {
			options,
			client,
			cache,
		};
		Ok(connection)
	}

	pub async fn reconnect(&mut self) -> Result<(), Error> {
		let (client, connection) =
			postgres::connect(self.options.url.as_str(), postgres::NoTls).await?;
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| tracing::error!(?error, "postgres connection failed"))
				.ok();
		});
		self.client = client;
		self.cache = Cache::default();
		Ok(())
	}

	pub fn cache(&self) -> &Cache {
		&self.cache
	}

	pub fn inner(&self) -> &postgres::Client {
		&self.client
	}

	pub fn inner_mut(&mut self) -> &mut postgres::Client {
		&mut self.client
	}
}

impl<'a> Transaction<'a> {
	#[must_use]
	pub fn cache(&self) -> &Cache {
		self.cache
	}

	#[must_use]
	pub fn inner(&self) -> &postgres::Transaction<'a> {
		&self.transaction
	}
}

impl super::Database for Database {
	type Error = Error;

	type Connection = pool::Guard<Connection>;

	async fn connection_with_options(
		&self,
		options: super::ConnectionOptions,
	) -> Result<Self::Connection, Self::Error> {
		let mut connection = self.pool.get(options.priority).await;
		if connection.client.is_closed() {
			connection.reconnect().await?;
		}
		Ok(connection)
	}
}

impl super::Connection for Connection {
	type Error = Error;

	type Transaction<'t>
		= Transaction<'t>
	where
		Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		let transaction = self.client.transaction().await?;
		let cache = &self.cache;
		Ok(Transaction { transaction, cache })
	}
}

impl super::Connection for pool::Guard<Connection> {
	type Error = Error;

	type Transaction<'t>
		= Transaction<'t>
	where
		Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		self.as_mut().transaction().await
	}
}

impl super::Transaction for Transaction<'_> {
	type Error = Error;

	async fn rollback(self) -> Result<(), Self::Error> {
		self.transaction.rollback().await?;
		Ok(())
	}

	async fn commit(self) -> Result<(), Self::Error> {
		self.transaction.commit().await?;
		Ok(())
	}
}

impl super::Query for Connection {
	type Error = Error;

	fn p(&self) -> &'static str {
		"$"
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> Result<u64, Self::Error> {
		execute(&self.client, &self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
		query(&self.client, &self.cache, statement, params).await
	}
}

impl super::Query for pool::Guard<Connection> {
	type Error = Error;

	fn p(&self) -> &'static str {
		self.as_ref().p()
	}

	fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<u64, Self::Error>> {
		self.as_ref().execute(statement, params)
	}

	fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error>>
	{
		self.as_ref().query(statement, params)
	}
}

impl super::Query for Transaction<'_> {
	type Error = Error;

	fn p(&self) -> &'static str {
		"$"
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> Result<u64, Self::Error> {
		execute(&self.transaction, self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
		query(&self.transaction, self.cache, statement, params).await
	}
}

impl super::Error for Error {
	fn is_retry(&self) -> bool {
		let Error::Postgres(error) = self else {
			return false;
		};
		error.code().map(tokio_postgres::error::SqlState::code) == Some("40001")
	}

	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

async fn execute(
	client: &impl postgres::GenericClient,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<Value>,
) -> Result<u64, Error> {
	let statement = cache.get(client, statement).await?;
	let params = &params
		.iter()
		.map(|value| value as &(dyn postgres::types::ToSql + Sync))
		.collect_vec();
	let n = client.execute(&statement, params).await?;
	Ok(n)
}

async fn query(
	client: &impl postgres::GenericClient,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<Value>,
) -> Result<impl Stream<Item = Result<Row, Error>> + Send, Error> {
	let statement = cache.get(client, statement).await?;
	let rows = client.query_raw(&statement, params).await?;
	let rows = rows
		.and_then(|row| {
			let mut entries = IndexMap::with_capacity(row.columns().len());
			for (i, column) in row.columns().iter().enumerate() {
				let name = column.name().to_owned();
				let value = row.get::<_, Value>(i);
				entries.insert(name, value);
			}
			let row = Row::with_entries(entries);
			future::ready(Ok(row))
		})
		.err_into();
	Ok(rows)
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
			postgres::types::Type::INT8 => Ok(Self::Integer(i64::from_sql(ty, raw)?)),
			postgres::types::Type::FLOAT8 => Ok(Self::Real(f64::from_sql(ty, raw)?)),
			postgres::types::Type::TEXT => Ok(Self::Text(String::from_sql(ty, raw)?)),
			postgres::types::Type::BYTEA => Ok(Self::Blob(<Vec<u8>>::from_sql(ty, raw)?)),
			_ => Err("invalid type".into()),
		}
	}

	fn from_sql_null(
		_: &postgres::types::Type,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(Self::Null)
	}

	postgres::types::accepts!(BOOL, INT8, NUMERIC, FLOAT8, TEXT, BYTEA);
}
