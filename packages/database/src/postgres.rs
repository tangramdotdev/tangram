#[cfg(feature = "tls")]
use rustls_platform_verifier::BuilderVerifierExt as _;
use {
	crate::{CacheKey, Error as _, Transaction as _},
	futures::{Stream, TryStreamExt as _, future, future::BoxFuture},
	indexmap::IndexMap,
	std::{borrow::Cow, collections::HashMap, ops::ControlFlow, time::Duration},
	tangram_pool::{self as pool, Pool},
	tangram_uri::Uri,
	tokio_postgres as postgres,
};

pub use postgres::types::Json;

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
	pub read: PoolOptions,
	pub retry: tangram_futures::retry::Options,
	pub write: PoolOptions,
}

#[derive(Clone, Debug)]
pub struct PoolOptions {
	pub max: usize,
	pub min: usize,
	pub ttl: Option<Duration>,
	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
	pub url: Uri,
}

pub struct Database {
	read_pool: Pool<Connection, Error>,
	retry: tangram_futures::retry::Options,
	write_pool: Pool<Connection, Error>,
}

#[derive(Default)]
pub struct Cache {
	statements: tokio::sync::Mutex<HashMap<CacheKey, postgres::Statement, fnv::FnvBuildHasher>>,
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
		let read_pool = create_pool(options.read).await?;
		let write_pool = create_pool(options.write).await?;
		let database = Self {
			read_pool,
			retry: options.retry,
			write_pool,
		};

		Ok(database)
	}

	#[must_use]
	pub fn read_pool(&self) -> &Pool<Connection, Error> {
		&self.read_pool
	}

	#[must_use]
	pub fn write_pool(&self) -> &Pool<Connection, Error> {
		&self.write_pool
	}

	pub async fn sync(&self) -> Result<(), Error> {
		Ok(())
	}

	pub async fn run<F, T, E>(&self, f: F) -> Result<T, Error>
	where
		for<'a, 'b> F:
			Fn(&'a Transaction<'b>) -> BoxFuture<'a, Result<ControlFlow<T, Error>, E>> + Sync,
		T: Send + 'static,
		E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
	{
		let options = self.retry.clone();
		tangram_futures::retry::retry(&options, || async {
			let mut connection = self
				.write_pool
				.get_exclusive(pool::Priority::default())
				.await?;
			if connection.client.is_closed() {
				connection.reconnect().await?;
			}
			let Connection { cache, client, .. } = &mut *connection;
			let inner = client
				.build_transaction()
				.isolation_level(postgres::IsolationLevel::Serializable)
				.start()
				.await?;
			let transaction = Transaction {
				cache,
				transaction: inner,
			};
			let value = match f(&transaction).await {
				Ok(ControlFlow::Break(value)) => value,
				Ok(ControlFlow::Continue(error)) => {
					return Ok(ControlFlow::Continue(error));
				},
				Err(error) => return Err(Error::other(error)),
			};
			let result = transaction.commit().await;
			match result {
				Ok(()) => Ok(ControlFlow::Break(value)),
				Err(error) if error.is_retry() => Ok(ControlFlow::Continue(error)),
				Err(error) => Err(error),
			}
		})
		.await
	}
}

async fn create_pool(options: PoolOptions) -> Result<Pool<Connection, Error>, Error> {
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
	let entry = pool::Options {
		min: options.min,
		max: options.max,
		shared: 1,
		ttl: options.ttl,
	};
	let pool = Pool::new(entry, create);
	for _ in 0..options.min {
		let connection = Connection::connect(connection_options.clone()).await?;
		pool.add(connection);
	}

	Ok(pool)
}

impl Connection {
	pub async fn connect(options: ConnectionOptions) -> Result<Self, Error> {
		let client = connect(&options.url).await?;
		let cache = Cache::default();
		let connection = Self {
			options,
			client,
			cache,
		};
		Ok(connection)
	}

	pub async fn reconnect(&mut self) -> Result<(), Error> {
		let client = connect(&self.options.url).await?;
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

async fn connect(url: &Uri) -> Result<postgres::Client, Error> {
	#[cfg(feature = "tls")]
	let (client, connection) = {
		// Create the TLS connector.
		let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
			rustls::crypto::aws_lc_rs::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.unwrap()
		.with_platform_verifier()
		.map_err(Error::other)?
		.with_no_client_auth();
		let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);

		postgres::connect(url.as_str(), tls).await?
	};
	#[cfg(not(feature = "tls"))]
	let (client, connection) = postgres::connect(url.as_str(), postgres::NoTls).await?;

	// Spawn the connection task.
	tokio::spawn(async move {
		connection
			.await
			.inspect_err(|error| tracing::error!(?error, "postgres connection failed"))
			.ok();
	});

	Ok(client)
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
		let pool = match options.kind {
			crate::ConnectionKind::Read => &self.read_pool,
			crate::ConnectionKind::Write => &self.write_pool,
		};
		let mut connection = pool.get_exclusive(options.priority).await?;
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
		let transaction = self
			.client
			.build_transaction()
			.isolation_level(postgres::IsolationLevel::Serializable)
			.start()
			.await?;
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
			Self::Other(_) => false,
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
		if matches!(ty.kind(), postgres::types::Kind::Enum(_)) {
			return match self {
				super::Value::Null => Ok(postgres::types::IsNull::Yes),
				super::Value::Text(value) => {
					out.extend_from_slice(value.as_bytes());
					Ok(postgres::types::IsNull::No)
				},
				_ => Err("expected a text value for a postgres enum".into()),
			};
		}
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

	fn accepts(ty: &postgres::types::Type) -> bool {
		matches!(
			*ty,
			postgres::types::Type::BOOL
				| postgres::types::Type::INT8
				| postgres::types::Type::FLOAT8
				| postgres::types::Type::TEXT
				| postgres::types::Type::BYTEA
		) || matches!(ty.kind(), postgres::types::Kind::Enum(_))
	}

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
			_ if matches!(ty.kind(), postgres::types::Kind::Enum(_)) => {
				Ok(Self::Text(std::str::from_utf8(raw)?.to_owned()))
			},
			_ => Err("invalid type".into()),
		}
	}

	fn from_sql_null(
		_: &postgres::types::Type,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(Self::Null)
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		matches!(
			*ty,
			postgres::types::Type::BOOL
				| postgres::types::Type::INT8
				| postgres::types::Type::NUMERIC
				| postgres::types::Type::FLOAT8
				| postgres::types::Type::TEXT
				| postgres::types::Type::BYTEA
		) || matches!(ty.kind(), postgres::types::Kind::Enum(_))
	}
}
