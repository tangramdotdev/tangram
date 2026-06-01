use {
	crate::CacheKey,
	futures::{Stream, TryStreamExt as _, future},
	indexmap::IndexMap,
	std::{borrow::Cow, collections::HashMap, time::Duration},
	tangram_pool::{self as pool, Pool},
	tangram_uri::Uri,
	tokio_postgres as postgres,
};

pub use {crate::run, postgres::types::Json};

pub mod row;
pub mod util;
pub mod value;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Postgres(postgres::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug)]
pub struct DatabaseOptions {
	pub max: usize,
	pub min: usize,
	pub retry: tangram_futures::retry::Options,
	pub ttl: Option<Duration>,
	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
	pub url: Uri,
}

#[derive(Default)]
pub struct Cache {
	statements: tokio::sync::Mutex<HashMap<CacheKey, postgres::Statement, fnv::FnvBuildHasher>>,
}

pub struct Database {
	pool: Pool<Connection, Error>,
	retry: tangram_futures::retry::Options,
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
		let key = CacheKey::new(statement);
		if let Some(statement) = self.statements.lock().await.get(&key) {
			return Ok(statement.clone());
		}
		let statement = client.prepare(key.as_str()).await?;
		self.statements.lock().await.insert(key, statement.clone());
		Ok(statement)
	}
}

impl Database {
	pub async fn new(options: DatabaseOptions) -> Result<Self, Error> {
		let connection_options = ConnectionOptions {
			url: options.url.clone(),
		};
		let create = {
			let connection_options = connection_options.clone();
			move || {
				let connection_options = connection_options.clone();
				async move { Connection::connect(connection_options).await }
			}
		};
		let pool = Pool::new(
			pool::Options {
				min: options.min,
				max: options.max,
				shared: 1,
				ttl: options.ttl,
			},
			create,
		);
		for _ in 0..options.min {
			let connection = Connection::connect(connection_options.clone()).await?;
			pool.add(connection);
		}
		let database = Self {
			pool,
			retry: options.retry,
		};
		Ok(database)
	}

	#[must_use]
	pub fn pool(&self) -> &Pool<Connection, Error> {
		&self.pool
	}

	pub async fn sync(&self) -> Result<(), Error> {
		Ok(())
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

	type Connection = pool::ExclusiveGuard<Connection, Error>;

	fn retry(&self) -> tangram_futures::retry::Options {
		self.retry.clone()
	}

	async fn connection_with_options(
		&self,
		options: super::ConnectionOptions,
	) -> Result<Self::Connection, Self::Error> {
		let mut connection = self.pool.get_exclusive(options.priority).await?;
		if connection.client.is_closed() {
			connection.reconnect().await?;
		}
		Ok(connection)
	}

	async fn sync(&self) -> Result<(), Self::Error> {
		self.sync().await
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

impl super::Connection for pool::ExclusiveGuard<Connection, Error> {
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
		params: Vec<super::Value>,
	) -> Result<u64, Self::Error> {
		execute(&self.client, &self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<impl Stream<Item = Result<super::Row, Self::Error>> + Send, Self::Error> {
		query(&self.client, &self.cache, statement, params).await
	}
}

impl super::Query for pool::ExclusiveGuard<Connection, Error> {
	type Error = Error;

	fn p(&self) -> &'static str {
		self.as_ref().p()
	}

	fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> impl Future<Output = Result<u64, Self::Error>> {
		self.as_ref().execute(statement, params)
	}

	fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<super::Row, Self::Error>> + Send, Self::Error>,
	> {
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
		params: Vec<super::Value>,
	) -> Result<u64, Self::Error> {
		execute(&self.transaction, self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<impl Stream<Item = Result<super::Row, Self::Error>> + Send, Self::Error> {
		query(&self.transaction, self.cache, statement, params).await
	}
}

impl super::Error for Error {
	fn is_retry(&self) -> bool {
		match self {
			Self::Postgres(error) => util::error_is_retryable(error),
			Self::Other(error) => {
				let mut current = Some(error.as_ref() as &dyn std::error::Error);
				while let Some(error) = current {
					if let Some(error) = error.downcast_ref::<Self>() {
						return error.is_retry();
					}
					current = error.source();
				}
				false
			},
		}
	}

	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

async fn execute(
	client: &impl postgres::GenericClient,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
) -> Result<u64, Error> {
	let statement = cache.get(client, statement).await?;
	let params = &params
		.iter()
		.map(|value| value as &(dyn postgres::types::ToSql + Sync))
		.collect::<Vec<_>>();
	let n = client.execute(&statement, params).await?;
	Ok(n)
}

async fn query(
	client: &impl postgres::GenericClient,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
) -> Result<impl Stream<Item = Result<super::Row, Error>> + Send, Error> {
	let statement = cache.get(client, statement).await?;
	let rows = client.query_raw(&statement, params).await?;
	let rows = rows
		.and_then(|row| {
			let mut entries = IndexMap::with_capacity(row.columns().len());
			for (i, column) in row.columns().iter().enumerate() {
				let name = column.name().to_owned();
				let value = row.get::<_, super::Value>(i);
				entries.insert(name, value);
			}
			let row = super::Row::with_entries(entries);
			future::ready(Ok(row))
		})
		.err_into();
	Ok(rows)
}

impl postgres::types::ToSql for super::Value {
	fn to_sql(
		&self,
		ty: &postgres::types::Type,
		out: &mut bytes::BytesMut,
	) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
	where
		Self: Sized,
	{
		match self {
			super::Value::Null => Ok(postgres::types::IsNull::Yes),
			super::Value::Integer(value) => {
				if *ty == postgres::types::Type::BOOL {
					(*value != 0).to_sql(ty, out)
				} else {
					value.to_sql(ty, out)
				}
			},
			super::Value::Real(value) => value.to_sql(ty, out),
			super::Value::Text(value) => value.to_sql(ty, out),
			super::Value::Blob(value) => value.as_ref().to_sql(ty, out),
		}
	}

	postgres::types::accepts!(BOOL, INT8, FLOAT8, TEXT, BYTEA);

	postgres::types::to_sql_checked!();
}

impl<'a> postgres::types::FromSql<'a> for super::Value {
	fn from_sql(
		ty: &postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		match *ty {
			postgres::types::Type::BOOL => Ok(Self::Integer(bool::from_sql(ty, raw)?.into())),
			postgres::types::Type::INT8 => Ok(Self::Integer(i64::from_sql(ty, raw)?)),
			postgres::types::Type::FLOAT8 => Ok(Self::Real(f64::from_sql(ty, raw)?)),
			postgres::types::Type::TEXT => Ok(Self::Text(String::from_sql(ty, raw)?)),
			postgres::types::Type::BYTEA => Ok(Self::Blob(Vec::<u8>::from_sql(ty, raw)?.into())),
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
