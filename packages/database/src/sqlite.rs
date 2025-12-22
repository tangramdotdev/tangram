use {
	crate::{
		CacheKey, Error as _,
		pool::{self, Pool},
	},
	bytes::Bytes,
	futures::{Stream, stream},
	indexmap::IndexMap,
	itertools::Itertools as _,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	std::{borrow::Cow, collections::HashMap, path::PathBuf, sync::Arc},
};

pub mod row;
pub mod value;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Sqlite(sqlite::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

type Initialize = Arc<dyn Fn(&sqlite::Connection) -> sqlite::Result<()> + Send + Sync + 'static>;

pub struct DatabaseOptions {
	pub connections: usize,
	pub initialize: Initialize,
	pub path: PathBuf,
}

pub struct Database {
	options: DatabaseOptions,
	read_pool: Pool<Connection>,
	write_pool: Pool<Connection>,
}

#[derive(Default)]
pub struct Cache {
	statements:
		std::sync::Mutex<HashMap<CacheKey, sqlite::Statement<'static>, fnv::FnvBuildHasher>>,
}

pub struct CachedStatement<'a> {
	cache: &'a Cache,
	key: CacheKey,
	statement: Option<sqlite::Statement<'static>>,
}

pub struct Connection {
	#[expect(dead_code)]
	options: ConnectionOptions,
	sender: tokio::sync::mpsc::UnboundedSender<ConnectionMessage>,
}

pub struct ConnectionOptions {
	pub flags: rusqlite::OpenFlags,
	pub initialize: Initialize,
	pub path: PathBuf,
}

pub struct Transaction<'a> {
	marker: std::marker::PhantomData<&'a ()>,
	sender: tokio::sync::mpsc::UnboundedSender<TransactionMessage>,
}

enum ConnectionMessage {
	Execute(ExecuteMessage),
	Query(QueryMessage),
	QueryAll(QueryAllMessage),
	Transaction(ConnectionTransactionMessage),
	With(ConnectionWithMessage),
}

type ConnectionWithMessage = Box<dyn FnOnce(&mut sqlite::Connection, &Cache) + Send>;

struct ConnectionTransactionMessage {
	sender: tokio::sync::oneshot::Sender<
		Result<tokio::sync::mpsc::UnboundedSender<TransactionMessage>, Error>,
	>,
}

enum TransactionMessage {
	Execute(ExecuteMessage),
	Query(QueryMessage),
	QueryAll(QueryAllMessage),
	Rollback(TransactionRollbackMessage),
	Commit(TransactionCommitMessage),
	With(TransactionWithMessage),
}

type TransactionWithMessage = Box<dyn FnOnce(&mut sqlite::Transaction, &Cache) + Send>;

struct TransactionRollbackMessage {
	sender: tokio::sync::oneshot::Sender<Result<(), Error>>,
}

struct TransactionCommitMessage {
	sender: tokio::sync::oneshot::Sender<Result<(), Error>>,
}

struct ExecuteMessage {
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
	sender: tokio::sync::oneshot::Sender<Result<u64, Error>>,
}

struct QueryMessage {
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
	sender: QueryMessageSender,
}

type QueryMessageSender = tokio::sync::oneshot::Sender<Result<QueryMessageRowSender, Error>>;

type QueryMessageRowSender = tokio::sync::mpsc::UnboundedSender<
	tokio::sync::oneshot::Sender<Result<Option<super::Row>, Error>>,
>;

struct QueryAllMessage {
	statement: Cow<'static, str>,
	params: Vec<super::Value>,
	sender: tokio::sync::oneshot::Sender<Result<Vec<super::Row>, Error>>,
}

impl Database {
	pub async fn new(options: DatabaseOptions) -> Result<Self, Error> {
		let write_pool = Pool::new();
		let flags = rusqlite::OpenFlags::default();
		let options_ = ConnectionOptions {
			flags,
			initialize: options.initialize.clone(),
			path: options.path.clone(),
		};
		let connection = Connection::connect(options_).await?;
		write_pool.add(connection);
		let read_pool = Pool::new();
		for _ in 0..options.connections {
			let mut flags = rusqlite::OpenFlags::default();
			flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_CREATE);
			flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE);
			flags.insert(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY);
			let options = ConnectionOptions {
				flags,
				initialize: options.initialize.clone(),
				path: options.path.clone(),
			};
			let connection = Connection::connect(options).await?;
			read_pool.add(connection);
		}
		let database = Self {
			options,
			read_pool,
			write_pool,
		};
		Ok(database)
	}

	#[must_use]
	pub fn read_pool(&self) -> &Pool<Connection> {
		&self.read_pool
	}

	#[must_use]
	pub fn write_pool(&self) -> &Pool<Connection> {
		&self.write_pool
	}

	pub fn create_connection(&self, readonly: bool) -> Result<sqlite::Connection, Error> {
		let mut flags = rusqlite::OpenFlags::default();
		flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_CREATE);
		if readonly {
			flags.remove(rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE);
			flags.insert(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY);
		}
		let connection = sqlite::Connection::open_with_flags(&self.options.path, flags)?;
		Ok(connection)
	}

	pub async fn sync(&self) -> Result<(), Error> {
		let connection = self.write_pool.get(crate::pool::Priority::default()).await;
		connection
			.with(|connection, _cache| {
				connection.pragma_update(None, "wal_checkpoint", "full")?;
				Ok::<_, sqlite::Error>(())
			})
			.await
			.map_err(Into::into)
	}
}

impl Connection {
	pub async fn connect(options: ConnectionOptions) -> Result<Self, Error> {
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let connection = sqlite::Connection::open_with_flags(&options.path, options.flags)?;
		(options.initialize)(&connection)?;
		std::thread::spawn(|| Self::run(connection, receiver));
		let connection = Self { options, sender };
		Ok(connection)
	}

	fn run(
		mut connection: sqlite::Connection,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<ConnectionMessage>,
	) {
		let cache = Cache::default();
		while let Some(message) = receiver.blocking_recv() {
			match message {
				ConnectionMessage::Execute(message) => {
					handle_execute_message(&connection, &cache, message);
				},
				ConnectionMessage::Query(message) => {
					handle_query_message(&connection, &cache, message);
				},
				ConnectionMessage::QueryAll(message) => {
					handle_query_all_message(&connection, &cache, message);
				},
				ConnectionMessage::Transaction(message) => {
					Transaction::run(&mut connection, &cache, message.sender);
				},
				ConnectionMessage::With(f) => {
					f(&mut connection, &cache);
				},
			}
		}
	}

	pub async fn with<F, T, E>(&self, f: F) -> Result<T, E>
	where
		F: FnOnce(&mut sqlite::Connection, &Cache) -> Result<T, E> + Send + 'static,
		T: Send + 'static,
		E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::With(Box::new(move |connection, cache| {
			sender.send(f(connection, cache)).map_err(|_| ()).ok();
		}));
		self.sender.send(message).unwrap();
		receiver.await.unwrap()
	}
}

impl Transaction<'_> {
	fn run(
		connection: &mut sqlite::Connection,
		cache: &Cache,
		sender: tokio::sync::oneshot::Sender<
			Result<tokio::sync::mpsc::UnboundedSender<TransactionMessage>, Error>,
		>,
	) {
		let mut transaction = match connection.transaction() {
			Ok(transaction) => transaction,
			Err(error) => {
				sender.send(Err(error.into())).ok();
				return;
			},
		};
		let (sender_, mut receiver) = tokio::sync::mpsc::unbounded_channel::<TransactionMessage>();
		sender.send(Ok(sender_)).ok();
		while let Some(message) = receiver.blocking_recv() {
			match message {
				TransactionMessage::Execute(message) => {
					handle_execute_message(&transaction, cache, message);
				},
				TransactionMessage::Query(message) => {
					handle_query_message(&transaction, cache, message);
				},
				TransactionMessage::QueryAll(message) => {
					handle_query_all_message(&transaction, cache, message);
				},
				TransactionMessage::Rollback(message) => {
					let result = transaction.rollback().map_err(Into::into);
					message.sender.send(result).ok();
					return;
				},
				TransactionMessage::Commit(message) => {
					let result = transaction.commit().map_err(Into::into);
					message.sender.send(result).ok();
					return;
				},
				TransactionMessage::With(f) => {
					f(&mut transaction, cache);
				},
			}
		}
		transaction.rollback().ok();
	}

	pub async fn with<F, T, E>(&self, f: F) -> Result<T, E>
	where
		F: FnOnce(&mut sqlite::Transaction, &Cache) -> Result<T, E> + Send + 'static,
		T: Send + 'static,
		E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::With(Box::new(|connection, cache| {
			sender.send(f(connection, cache)).map_err(|_| ()).ok();
		}));
		self.sender.send(message).unwrap();
		receiver.await.unwrap()
	}
}

impl super::Error for Error {
	fn is_retry(&self) -> bool {
		false
	}

	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl super::Database for Database {
	type Error = Error;

	type Connection = pool::Guard<Connection>;

	async fn connection_with_options(
		&self,
		options: super::ConnectionOptions,
	) -> Result<Self::Connection, Self::Error> {
		let connection = match options.kind {
			crate::ConnectionKind::Read => self.read_pool.get(options.priority).await,
			crate::ConnectionKind::Write => self.write_pool.get(options.priority).await,
		};
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
		let marker = std::marker::PhantomData;
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::Transaction(ConnectionTransactionMessage { sender });
		self.sender.send(message).unwrap();
		let sender = receiver.await.unwrap()?;
		Ok(Transaction { marker, sender })
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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::Rollback(TransactionRollbackMessage { sender });
		self.sender.send(message).unwrap();
		receiver.await.unwrap()?;
		Ok(())
	}

	async fn commit(self) -> Result<(), Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::Commit(TransactionCommitMessage { sender });
		self.sender.send(message).unwrap();
		receiver.await.unwrap()?;
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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::Execute(ExecuteMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let n = receiver.await.unwrap()?;
		Ok(n)
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<impl Stream<Item = Result<super::Row, Self::Error>> + Send, Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::Query(QueryMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let sender = receiver.await.unwrap()?;
		let rows = stream::try_unfold(sender, |sender| async move {
			let (sender_, receiver) = tokio::sync::oneshot::channel();
			sender
				.send(sender_)
				.map_err(|_| Error::other("failed to get the next row"))?;
			let result = receiver
				.await
				.map_err(|_| Error::other("failed to get the next row"))?;
			match result {
				Ok(Some(row)) => Ok(Some((row, sender))),
				Ok(None) => Ok(None),
				Err(error) => Err(error),
			}
		});
		Ok(rows)
	}

	async fn query_optional(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<Option<super::Row>, Self::Error> {
		Ok(self.query_all(statement, params).await?.into_iter().next())
	}

	async fn query_one(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<super::Row, Self::Error> {
		self.query_optional(statement, params)
			.await?
			.ok_or_else(|| Error::other("expected a row"))
	}

	async fn query_all(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<Vec<super::Row>, Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::QueryAll(QueryAllMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let rows = receiver.await.unwrap()?;
		Ok(rows)
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

	fn query_optional(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> impl Future<Output = Result<Option<super::Row>, Self::Error>> {
		self.as_ref().query_optional(statement, params)
	}

	fn query_one(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> impl Future<Output = Result<super::Row, Self::Error>> {
		self.as_ref().query_one(statement, params)
	}

	fn query_all(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> impl Future<Output = Result<Vec<super::Row>, Self::Error>> {
		self.as_ref().query_all(statement, params)
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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::Execute(ExecuteMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let n = receiver.await.unwrap()?;
		Ok(n)
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<impl Stream<Item = Result<super::Row, Self::Error>> + Send, Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::Query(QueryMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let sender = receiver.await.unwrap()?;
		let rows = stream::try_unfold(sender, |sender| async move {
			let (sender_, receiver) = tokio::sync::oneshot::channel();
			sender
				.send(sender_)
				.map_err(|_| Error::other("failed to get the next row"))?;
			let result = receiver
				.await
				.map_err(|_| Error::other("failed to get the next row"))?;
			match result {
				Ok(Some(row)) => Ok(Some((row, sender))),
				Ok(None) => Ok(None),
				Err(error) => Err(error),
			}
		});
		Ok(rows)
	}

	async fn query_optional(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<Option<super::Row>, Self::Error> {
		Ok(self.query_all(statement, params).await?.into_iter().next())
	}

	async fn query_one(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<super::Row, Self::Error> {
		self.query_optional(statement, params)
			.await?
			.ok_or_else(|| Error::other("expected a row"))
	}

	async fn query_all(
		&self,
		statement: Cow<'static, str>,
		params: Vec<super::Value>,
	) -> Result<Vec<super::Row>, Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::QueryAll(QueryAllMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let rows = receiver.await.unwrap()?;
		Ok(rows)
	}
}

fn handle_execute_message(connection: &sqlite::Connection, cache: &Cache, message: ExecuteMessage) {
	let ExecuteMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match cache.get(connection, statement) {
		Ok(statement) => statement,
		Err(error) => {
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Execute the statement.
	let n = match statement.execute(sqlite::params_from_iter(params)) {
		Ok(n) => n.to_u64().unwrap(),
		Err(error) => {
			sender.send(Err(error.into())).ok();
			return;
		},
	};

	// Send the result.
	sender.send(Ok(n)).ok();
}

fn handle_query_message(connection: &sqlite::Connection, cache: &Cache, message: QueryMessage) {
	let QueryMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match cache.get(connection, statement) {
		Ok(statement) => statement,
		Err(error) => {
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Get the columns.
	let columns = statement
		.column_names()
		.into_iter()
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>();

	// Execute the statement.
	let mut rows = match statement.query(sqlite::params_from_iter(params)) {
		Ok(rows) => rows,
		Err(error) => {
			sender.send(Err(error.into())).ok();
			return;
		},
	};

	// Create a channel.
	let (sender_, mut receiver) = tokio::sync::mpsc::unbounded_channel();

	// Send the sender.
	sender.send(Ok(sender_)).ok();

	// Send each row.
	'a: while let Some(sender) = receiver.blocking_recv() {
		let row = match rows.next() {
			Ok(Some(row)) => row,
			Ok(None) => {
				sender.send(Ok(None)).ok();
				continue;
			},
			Err(error) => {
				sender.send(Err(error.into())).ok();
				continue;
			},
		};
		let mut entries = IndexMap::with_capacity(columns.len());
		for (i, column) in columns.iter().enumerate() {
			let name = column.to_owned();
			let value = match row.get::<_, super::Value>(i) {
				Ok(value) => value,
				Err(error) => {
					sender.send(Err(error.into())).ok();
					continue 'a;
				},
			};
			entries.insert(name, value);
		}
		let row = super::Row::with_entries(entries);
		sender.send(Ok(Some(row))).ok();
	}
}

fn handle_query_all_message(
	connection: &sqlite::Connection,
	cache: &Cache,
	message: QueryAllMessage,
) {
	let QueryAllMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match cache.get(connection, statement) {
		Ok(statement) => statement,
		Err(error) => {
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Get the column names.
	let column_names = statement
		.column_names()
		.into_iter()
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>();

	// Execute the statement.
	let rows = match statement.query(sqlite::params_from_iter(params)) {
		Ok(rows) => rows,
		Err(error) => {
			sender.send(Err(error.into())).ok();
			return;
		},
	};

	// Collect the rows.
	let result = rows
		.and_then(|row| {
			let mut entries = IndexMap::with_capacity(column_names.len());
			for (i, column) in column_names.iter().enumerate() {
				let name = column.to_owned();
				let value = row.get::<_, super::Value>(i)?;
				entries.insert(name, value);
			}
			let row = super::Row::with_entries(entries);
			Ok::<_, sqlite::Error>(row)
		})
		.try_collect()
		.map_err(Into::into);

	// Send the result.
	sender.send(result).ok();
}

impl sqlite::types::ToSql for super::Value {
	fn to_sql(&self) -> sqlite::Result<sqlite::types::ToSqlOutput<'_>> {
		match self {
			super::Value::Null => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Null,
			)),
			super::Value::Integer(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Integer(*value),
			)),
			super::Value::Real(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Real(*value),
			)),
			super::Value::Text(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Text(value.as_bytes()),
			)),
			super::Value::Blob(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Blob(value.as_ref()),
			)),
		}
	}
}

impl sqlite::types::FromSql for super::Value {
	fn column_result(value: sqlite::types::ValueRef) -> sqlite::types::FromSqlResult<Self> {
		match value {
			sqlite::types::ValueRef::Null => Ok(Self::Null),
			sqlite::types::ValueRef::Integer(value) => Ok(Self::Integer(value)),
			sqlite::types::ValueRef::Real(value) => Ok(Self::Real(value)),
			sqlite::types::ValueRef::Text(value) => Ok(Self::Text(
				String::from_utf8(value.to_owned())
					.map_err(|error| sqlite::types::FromSqlError::Other(error.into()))?,
			)),
			sqlite::types::ValueRef::Blob(value) => Ok(Self::Blob(Bytes::copy_from_slice(value))),
		}
	}
}

impl Cache {
	pub fn get<'a>(
		&'a self,
		connection: &'a sqlite::Connection,
		statement: Cow<'static, str>,
	) -> Result<CachedStatement<'a>, Error> {
		let key = CacheKey::new(statement);
		let stmt = {
			let mut guard = self.statements.lock().unwrap();
			if let Some(stmt) = guard.remove(&key) {
				stmt
			} else {
				let stmt = connection.prepare(key.as_str())?;
				unsafe {
					std::mem::transmute::<sqlite::Statement<'_>, sqlite::Statement<'static>>(stmt)
				}
			}
		};
		Ok(CachedStatement {
			cache: self,
			key,
			statement: Some(stmt),
		})
	}
}

impl Drop for CachedStatement<'_> {
	fn drop(&mut self) {
		if let Some(stmt) = self.statement.take() {
			let mut guard = self.cache.statements.lock().unwrap();
			guard.insert(self.key.clone(), stmt);
		}
	}
}

impl std::ops::Deref for CachedStatement<'_> {
	type Target = sqlite::Statement<'static>;

	fn deref(&self) -> &Self::Target {
		self.statement.as_ref().unwrap()
	}
}

impl std::ops::DerefMut for CachedStatement<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.statement.as_mut().unwrap()
	}
}
