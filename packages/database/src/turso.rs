use {
	crate::CacheKey,
	bytes::Bytes,
	futures::{Stream, stream},
	indexmap::IndexMap,
	std::{
		borrow::Cow, collections::HashMap, future::Future, path::PathBuf, pin::Pin, sync::Arc,
		time::Duration,
	},
	tangram_pool::{self as pool, Pool},
};

use {crate::Query, futures::TryStreamExt as _};

pub use crate::run;

pub mod row;
pub mod value;

pub type Initialize = Arc<
	dyn for<'a> Fn(
			&'a turso::Connection,
		) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>>
		+ Send
		+ Sync,
>;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Turso(turso::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

pub struct DatabaseOptions {
	pub initialize: Initialize,
	pub max: usize,
	pub min: usize,
	pub path: PathBuf,
	pub retry: tangram_futures::retry::Options,
	pub ttl: Option<Duration>,
}

#[derive(Default)]
pub struct Cache {
	statements: tokio::sync::Mutex<HashMap<CacheKey, turso::Statement, fnv::FnvBuildHasher>>,
}

pub struct Database {
	#[expect(dead_code)]
	db: turso::Database,
	pool: Pool<Connection, Error>,
	retry: tangram_futures::retry::Options,
}

pub struct Connection {
	connection: turso::Connection,
	cache: Cache,
}

pub struct Transaction<'a> {
	transaction: turso::transaction::Transaction<'a>,
	cache: &'a Cache,
}

impl Cache {
	pub async fn get(
		&self,
		transaction: &turso::transaction::Transaction<'_>,
		statement: Cow<'static, str>,
	) -> Result<turso::Statement, Error> {
		let key = CacheKey::new(statement);
		if let Some(statement) = self.statements.lock().await.get(&key) {
			return Ok(statement.clone());
		}
		let statement = transaction.prepare(key.as_str()).await?;
		self.statements.lock().await.insert(key, statement.clone());
		Ok(statement)
	}
}

impl Database {
	pub async fn new(options: DatabaseOptions) -> Result<Self, Error> {
		let path = options
			.path
			.to_str()
			.ok_or_else(|| Error::Other("the path is not valid UTF-8".into()))?;
		let db = turso::Builder::new_local(path).build().await?;
		let initialize = options.initialize.clone();
		let create = {
			let db = db.clone();
			let initialize = initialize.clone();
			move || {
				let db = db.clone();
				let initialize = initialize.clone();
				async move { Connection::connect(&db, &initialize).await }
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
			let connection = Connection::connect(&db, &options.initialize).await?;
			pool.add(connection);
		}
		let database = Self {
			db,
			pool,
			retry: options.retry,
		};
		Ok(database)
	}

	#[must_use]
	pub fn pool(&self) -> &Pool<Connection, Error> {
		&self.pool
	}
}

impl Connection {
	pub async fn connect(
		database: &turso::Database,
		initialize: &Initialize,
	) -> Result<Self, Error> {
		let connection = database.connect()?;
		initialize(&connection).await?;
		let cache = Cache::default();
		Ok(Self { connection, cache })
	}

	pub fn cache(&self) -> &Cache {
		&self.cache
	}

	pub fn inner(&self) -> &turso::Connection {
		&self.connection
	}

	pub fn inner_mut(&mut self) -> &mut turso::Connection {
		&mut self.connection
	}
}

impl<'a> Transaction<'a> {
	#[must_use]
	pub fn cache(&self) -> &Cache {
		self.cache
	}

	#[must_use]
	pub fn inner(&self) -> &turso::transaction::Transaction<'a> {
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
		let connection = self.pool.get_exclusive(options.priority).await?;
		Ok(connection)
	}

	async fn sync(&self) -> Result<(), Self::Error> {
		let connection = self
			.pool
			.get_exclusive(tangram_pool::Priority::default())
			.await?;
		connection
			.query("pragma wal_checkpoint(full)".into(), vec![])
			.await?
			.try_collect::<Vec<_>>()
			.await?;
		Ok(())
	}
}

impl super::Connection for Connection {
	type Error = Error;

	type Transaction<'t>
		= Transaction<'t>
	where
		Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		let transaction = self.connection.transaction().await?;
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
		"?"
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<u64, Self::Error> {
		let transaction = turso::transaction::Transaction::new_unchecked(
			&self.connection,
			turso::transaction::TransactionBehavior::Deferred,
		)
		.await?;
		let n = execute(&transaction, &self.cache, statement, params).await?;
		transaction.commit().await?;
		Ok(n)
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<impl Stream<Item = Result<super::Row, Self::Error>> + Send, Self::Error> {
		let transaction = turso::transaction::Transaction::new_unchecked(
			&self.connection,
			turso::transaction::TransactionBehavior::Deferred,
		)
		.await?;
		let rows = query_rows(&transaction, &self.cache, statement, params).await?;
		Ok(stream::iter(rows.into_iter().map(Ok)))
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
		"?"
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
		let rows = query_rows(&self.transaction, self.cache, statement, params).await?;
		Ok(stream::iter(rows.into_iter().map(Ok)))
	}
}

impl super::Error for Error {
	fn is_retry(&self) -> bool {
		match self {
			Self::Turso(error) => {
				matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
			},
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
	transaction: &turso::transaction::Transaction<'_>,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
) -> Result<u64, Error> {
	let params: Vec<turso::Value> = params.into_iter().map(into_turso_value).collect();
	let mut statement = cache.get(transaction, statement).await?;
	let n = statement.execute(params).await?;
	Ok(n)
}

async fn query_rows(
	transaction: &turso::transaction::Transaction<'_>,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
) -> Result<Vec<super::Row>, Error> {
	let params: Vec<turso::Value> = params.into_iter().map(into_turso_value).collect();
	let mut statement = cache.get(transaction, statement).await?;
	let column_names = statement.column_names();
	let mut rows = statement.query(params).await?;
	let mut results = Vec::new();
	while let Some(row) = rows.next().await? {
		let mut entries = IndexMap::with_capacity(column_names.len());
		for (i, name) in column_names.iter().enumerate() {
			let value = from_turso_value(row.get_value(i)?);
			entries.insert(name.clone(), value);
		}
		results.push(super::Row::with_entries(entries));
	}
	Ok(results)
}

fn into_turso_value(value: super::Value) -> turso::Value {
	match value {
		super::Value::Null => turso::Value::Null,
		super::Value::Integer(v) => turso::Value::Integer(v),
		super::Value::Real(v) => turso::Value::Real(v),
		super::Value::Text(v) => turso::Value::Text(v),
		super::Value::Blob(bytes) => turso::Value::Blob(bytes.to_vec()),
	}
}

pub(crate) fn from_turso_value(value: turso::Value) -> super::Value {
	match value {
		turso::Value::Null => super::Value::Null,
		turso::Value::Integer(v) => super::Value::Integer(v),
		turso::Value::Real(v) => super::Value::Real(v),
		turso::Value::Text(v) => super::Value::Text(v),
		turso::Value::Blob(v) => super::Value::Blob(Bytes::from(v)),
	}
}
