#[cfg(feature = "tls")]
use rustls_platform_verifier::BuilderVerifierExt as _;
use {
	futures::{Stream, TryStreamExt as _, future, future::BoxFuture},
	indexmap::IndexMap,
	std::{borrow::Cow, collections::HashMap, ops::ControlFlow, time::Duration},
	tangram_database::{self as db, CacheKey, Error as _, Transaction as _},
	tangram_pool::{self as pool, Pool},
	tangram_uri::Uri,
	tokio_postgres as postgres,
};

pub use json::Json;

pub mod json;
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

pub struct Guard(pool::ExclusiveGuard<Connection, Error>);

pub struct Connection {
	options: ConnectionOptions,
	client: postgres::Client,
	cache: Cache,
}

pub struct Transaction<'a> {
	transaction: postgres::Transaction<'a>,
	cache: &'a Cache,
}

#[derive(Debug)]
struct SqlValue(db::Value);

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

impl std::ops::Deref for Guard {
	type Target = Connection;
	fn deref(&self) -> &Connection {
		&self.0
	}
}

impl std::ops::DerefMut for Guard {
	fn deref_mut(&mut self) -> &mut Connection {
		&mut self.0
	}
}

impl db::Database for Database {
	type Error = Error;

	type Connection = Guard;

	fn retry(&self) -> tangram_futures::retry::Options {
		self.retry.clone()
	}

	async fn connection_with_options(
		&self,
		options: db::ConnectionOptions,
	) -> Result<Self::Connection, Self::Error> {
		let pool = match options.kind {
			db::ConnectionKind::Read => &self.read_pool,
			db::ConnectionKind::Write => &self.write_pool,
		};
		let mut connection = pool.get_exclusive(options.priority).await?;
		if connection.client.is_closed() {
			connection.reconnect().await?;
		}
		Ok(Guard(connection))
	}

	async fn sync(&self) -> Result<(), Self::Error> {
		self.sync().await
	}
}

impl db::Connection for Connection {
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

impl db::Connection for Guard {
	type Error = Error;

	type Transaction<'t>
		= Transaction<'t>
	where
		Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		self.0.as_mut().transaction().await
	}
}

impl db::Transaction for Transaction<'_> {
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

impl db::Query for Connection {
	type Error = Error;

	fn p(&self) -> &'static str {
		"$"
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<u64, Self::Error> {
		execute(&self.client, &self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		query(&self.client, &self.cache, statement, params).await
	}
}

impl db::Query for Guard {
	type Error = Error;

	fn p(&self) -> &'static str {
		self.0.as_ref().p()
	}

	fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> impl Future<Output = Result<u64, Self::Error>> {
		self.0.as_ref().execute(statement, params)
	}

	fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error>>
	{
		self.0.as_ref().query(statement, params)
	}
}

impl db::Query for Transaction<'_> {
	type Error = Error;

	fn p(&self) -> &'static str {
		"$"
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<u64, Self::Error> {
		execute(&self.transaction, self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		query(&self.transaction, self.cache, statement, params).await
	}
}

impl db::Error for Error {
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
	params: Vec<db::Value>,
) -> Result<u64, Error> {
	let statement = cache.get(client, statement).await?;
	let params = params.into_iter().map(SqlValue).collect::<Vec<_>>();
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
	params: Vec<db::Value>,
) -> Result<impl Stream<Item = Result<db::Row, Error>> + Send, Error> {
	let statement = cache.get(client, statement).await?;
	let params = params.into_iter().map(SqlValue).collect::<Vec<_>>();
	let rows = client.query_raw(&statement, params).await?;
	let rows = rows
		.and_then(|row| {
			let mut entries = IndexMap::with_capacity(row.columns().len());
			for (i, column) in row.columns().iter().enumerate() {
				let name = column.name().to_owned();
				let value = row.get::<_, SqlValue>(i).0;
				entries.insert(name, value);
			}
			let row = db::Row::with_entries(entries);
			future::ready(Ok(row))
		})
		.err_into();
	Ok(rows)
}

impl postgres::types::ToSql for SqlValue {
	fn to_sql(
		&self,
		ty: &postgres::types::Type,
		out: &mut bytes::BytesMut,
	) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
	where
		Self: Sized,
	{
		if matches!(ty.kind(), postgres::types::Kind::Enum(_)) {
			return match &self.0 {
				db::Value::Null => Ok(postgres::types::IsNull::Yes),
				db::Value::Text(value) => {
					out.extend_from_slice(value.as_bytes());
					Ok(postgres::types::IsNull::No)
				},
				_ => Err("expected a text value for a postgres enum".into()),
			};
		}
		match &self.0 {
			db::Value::Null => Ok(postgres::types::IsNull::Yes),
			db::Value::Integer(value) => {
				if *ty == postgres::types::Type::BOOL {
					(*value != 0).to_sql(ty, out)
				} else {
					value.to_sql(ty, out)
				}
			},
			db::Value::Real(value) => value.to_sql(ty, out),
			db::Value::Text(value) => value.to_sql(ty, out),
			db::Value::Blob(value) => value.as_ref().to_sql(ty, out),
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

impl<'a> postgres::types::FromSql<'a> for SqlValue {
	fn from_sql(
		ty: &postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		match *ty {
			postgres::types::Type::BOOL => {
				Ok(Self(db::Value::Integer(bool::from_sql(ty, raw)?.into())))
			},
			postgres::types::Type::INT8 => Ok(Self(db::Value::Integer(i64::from_sql(ty, raw)?))),
			postgres::types::Type::FLOAT8 => Ok(Self(db::Value::Real(f64::from_sql(ty, raw)?))),
			postgres::types::Type::TEXT => Ok(Self(db::Value::Text(String::from_sql(ty, raw)?))),
			postgres::types::Type::BYTEA => {
				Ok(Self(db::Value::Blob(Vec::<u8>::from_sql(ty, raw)?.into())))
			},
			_ if matches!(ty.kind(), postgres::types::Kind::Enum(_)) => {
				Ok(Self(db::Value::Text(std::str::from_utf8(raw)?.to_owned())))
			},
			_ => Err("invalid type".into()),
		}
	}

	fn from_sql_null(
		_: &postgres::types::Type,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(Self(db::Value::Null))
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

#[cfg(test)]
mod tests {
	use super::*;
	use postgres::types::{FromSql as _, ToSql as _};

	#[test]
	fn sql_value_round_trip() {
		let values = [
			(db::Value::Null, postgres::types::Type::TEXT),
			(db::Value::Integer(42), postgres::types::Type::INT8),
			(db::Value::Real(1.5), postgres::types::Type::FLOAT8),
			(db::Value::Text("hello".into()), postgres::types::Type::TEXT),
			(
				db::Value::Blob(bytes::Bytes::from_static(b"hello")),
				postgres::types::Type::BYTEA,
			),
		];
		for (value, ty) in values {
			let mut raw = bytes::BytesMut::new();
			let is_null = SqlValue(value.clone()).to_sql(&ty, &mut raw).unwrap();
			let decoded = match is_null {
				postgres::types::IsNull::Yes => SqlValue::from_sql_null(&ty).unwrap(),
				postgres::types::IsNull::No => SqlValue::from_sql(&ty, &raw).unwrap(),
			};
			assert_eq!(
				serde_json::to_value(&decoded.0).unwrap(),
				serde_json::to_value(&value).unwrap()
			);
		}
	}

	#[test]
	fn sql_value_rejects_an_invalid_enum_value() {
		let value = SqlValue(db::Value::Integer(1));
		let mut raw = bytes::BytesMut::new();
		let ty = postgres::types::Type::new(
			"status".into(),
			1,
			postgres::types::Kind::Enum(vec!["ready".into()]),
			"public".into(),
		);
		assert!(value.to_sql(&ty, &mut raw).is_err());
	}
}
