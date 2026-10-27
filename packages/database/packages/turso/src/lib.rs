use {
	bytes::Bytes,
	futures::{Stream, TryStreamExt as _, future::BoxFuture, stream},
	indexmap::IndexMap,
	std::{
		borrow::Cow, collections::HashMap, ops::ControlFlow, path::PathBuf, pin::Pin, sync::Arc,
		time::Duration,
	},
	tangram_database::{
		self as db, CacheKey, Connection as _, Error as _, Query, Transaction as _,
	},
	tangram_pool::{self as pool, Pool},
};

pub use json::Json;

pub mod json;
pub mod row;
pub mod value;

pub type Initialize = Arc<
	dyn for<'a> Fn(
			&'a turso::Connection,
		) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>
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

pub struct Guard(pool::ExclusiveGuard<Connection, Error>);

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
		let entry = pool::Options {
			min: options.min,
			max: options.max,
			shared: 1,
			ttl: options.ttl,
		};
		let pool = Pool::new(entry, create);
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

	pub async fn run<F, T, E>(&self, f: F) -> Result<T, Error>
	where
		for<'a, 'b> F:
			Fn(&'a Transaction<'b>) -> BoxFuture<'a, Result<ControlFlow<T, Error>, E>> + Sync,
		T: Send + 'static,
		E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
	{
		let options = self.retry.clone();
		tangram_futures::retry::retry(&options, || async {
			let mut connection = self.pool.get_exclusive(pool::Priority::default()).await?;
			let transaction = connection.transaction().await?;
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
		let connection = self.pool.get_exclusive(options.priority).await?;
		Ok(Guard(connection))
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

impl db::Connection for Connection {
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
		"?"
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
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
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		let transaction = turso::transaction::Transaction::new_unchecked(
			&self.connection,
			turso::transaction::TransactionBehavior::Deferred,
		)
		.await?;
		let rows = query(&transaction, &self.cache, statement, params).await?;
		Ok(stream::iter(rows.into_iter().map(Ok)))
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
		"?"
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
		let rows = query(&self.transaction, self.cache, statement, params).await?;
		Ok(stream::iter(rows.into_iter().map(Ok)))
	}
}

impl db::Error for Error {
	fn is_retry(&self) -> bool {
		match self {
			Self::Turso(error) => {
				matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
			},
			Self::Other(_) => false,
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
	params: Vec<db::Value>,
) -> Result<u64, Error> {
	let params: Vec<turso::Value> = params.into_iter().map(into_turso_value).collect();
	let mut statement = cache.get(transaction, statement).await?;
	let n = statement.execute(params).await?;
	Ok(n)
}

async fn query(
	transaction: &turso::transaction::Transaction<'_>,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<db::Value>,
) -> Result<Vec<db::Row>, Error> {
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
		results.push(db::Row::with_entries(entries));
	}
	Ok(results)
}

fn into_turso_value(value: db::Value) -> turso::Value {
	match value {
		db::Value::Null => turso::Value::Null,
		db::Value::Integer(v) => turso::Value::Integer(v),
		db::Value::Real(v) => turso::Value::Real(v),
		db::Value::Text(v) => turso::Value::Text(v),
		db::Value::Blob(bytes) => turso::Value::Blob(bytes.to_vec()),
	}
}

pub(crate) fn from_turso_value(value: turso::Value) -> db::Value {
	match value {
		turso::Value::Null => db::Value::Null,
		turso::Value::Integer(v) => db::Value::Integer(v),
		turso::Value::Real(v) => db::Value::Real(v),
		turso::Value::Text(v) => db::Value::Text(v),
		turso::Value::Blob(v) => db::Value::Blob(Bytes::from(v)),
	}
}
