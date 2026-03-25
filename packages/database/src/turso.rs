use {
	crate::{
		CacheKey,
		pool::{self, Pool},
	},
	bytes::Bytes,
	futures::{Stream, stream},
	indexmap::IndexMap,
	std::{borrow::Cow, collections::HashMap, path::PathBuf},
};

pub mod row;
pub mod value;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Turso(turso::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

pub struct DatabaseOptions {
	pub connections: usize,
	pub path: PathBuf,
}

#[derive(Default)]
pub struct Cache {
	statements: tokio::sync::Mutex<HashMap<CacheKey, turso::Statement, fnv::FnvBuildHasher>>,
}

pub struct Database {
	#[expect(dead_code)]
	db: turso::Database,
	pool: Pool<Connection>,
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
		connection: &turso::Connection,
		statement: Cow<'static, str>,
	) -> Result<turso::Statement, Error> {
		let key = CacheKey::new(statement);
		if let Some(statement) = self.statements.lock().await.get(&key) {
			return Ok(statement.clone());
		}
		let statement = connection.prepare(key.as_str()).await?;
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
		let pool = Pool::new();
		for _ in 0..options.connections {
			let connection = Connection::connect(&db)?;
			pool.add(connection);
		}
		let database = Self { db, pool };
		Ok(database)
	}

	#[must_use]
	pub fn pool(&self) -> &Pool<Connection> {
		&self.pool
	}

	pub async fn sync(&self) -> Result<(), Error> {
		Ok(())
	}
}

impl Connection {
	pub fn connect(database: &turso::Database) -> Result<Self, Error> {
		let connection = database.connect()?;
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

	type Connection = pool::Guard<Connection>;

	async fn connection_with_options(
		&self,
		options: super::ConnectionOptions,
	) -> Result<Self::Connection, Self::Error> {
		let connection = self.pool.get(options.priority).await;
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
		let transaction = self.connection.transaction().await?;
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
		"?"
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<u64, Self::Error> {
		execute(&self.connection, &self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<impl Stream<Item = Result<super::Row, Self::Error>> + Send, Self::Error> {
		query(&self.connection, &self.cache, statement, params).await
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
		query(&self.transaction, self.cache, statement, params).await
	}
}

impl super::Error for Error {
	fn is_retry(&self) -> bool {
		matches!(
			self,
			Error::Turso(turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
		)
	}

	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

async fn execute(
	connection: &turso::Connection,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
) -> Result<u64, Error> {
	let params: Vec<turso::Value> = params.into_iter().map(into_turso_value).collect();
	let mut statement = cache.get(connection, statement).await?;
	let n = statement.execute(params).await?;
	Ok(n)
}

async fn query(
	connection: &turso::Connection,
	cache: &Cache,
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
) -> Result<impl Stream<Item = Result<super::Row, Error>> + Send, Error> {
	let params: Vec<turso::Value> = params.into_iter().map(into_turso_value).collect();
	let mut statement = cache.get(connection, statement).await?;
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
	Ok(stream::iter(results.into_iter().map(Ok)))
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
