use super::{Row, Value};
use futures::{stream, Stream};
use indexmap::IndexMap;
use itertools::Itertools;
use num::ToPrimitive;
use rusqlite as sqlite;
use std::{
	borrow::Cow,
	marker::PhantomData,
	path::{Path, PathBuf},
};
use tangram_error::{error, Result};

pub struct Database {
	pool: tangram_pool::Pool<Connection>,
}

pub struct Options {
	pub path: PathBuf,
	pub max_connections: usize,
}

pub struct Connection {
	sender: tokio::sync::mpsc::UnboundedSender<ConnectionMessage>,
}

enum ConnectionMessage {
	Execute(ExecuteMessage),
	Query(QueryMessage),
	QueryAll(QueryAllMessage),
	Transaction(
		tokio::sync::oneshot::Sender<
			Result<tokio::sync::mpsc::UnboundedSender<TransactionMessage>>,
		>,
	),
	With(Box<dyn FnOnce(&mut sqlite::Connection) + Send + Sync + 'static>),
}

struct ExecuteMessage {
	statement: String,
	params: Vec<Value>,
	sender: tokio::sync::oneshot::Sender<Result<u64>>,
}

struct QueryMessage {
	statement: String,
	params: Vec<Value>,
	sender: QueryMessageSender,
}

type QueryMessageSender = tokio::sync::oneshot::Sender<Result<QueryMessageRowRequestSender>>;

type QueryMessageRowRequestSender =
	tokio::sync::mpsc::UnboundedSender<QueryMessageRowResponseSender>;

type QueryMessageRowResponseSender = tokio::sync::oneshot::Sender<Result<Option<Row>>>;

struct QueryAllMessage {
	statement: String,
	params: Vec<Value>,
	sender: tokio::sync::oneshot::Sender<Result<Vec<Row>>>,
}

enum TransactionMessage {
	Execute(ExecuteMessage),
	Query(QueryMessage),
	QueryAll(QueryAllMessage),
	Commit(tokio::sync::oneshot::Sender<Result<()>>),
	Rollback(tokio::sync::oneshot::Sender<Result<()>>),
	With(Box<dyn FnOnce(&mut sqlite::Transaction) + Send + Sync + 'static>),
}

pub struct Transaction<'a> {
	_marker: PhantomData<&'a ()>,
	sender: tokio::sync::mpsc::UnboundedSender<TransactionMessage>,
}

pub struct Json<T>(pub T);

impl Database {
	pub async fn new(options: Options) -> Result<Self> {
		let mut connections = Vec::with_capacity(options.max_connections);
		for _ in 0..options.max_connections {
			let client = Connection::connect(&options.path).await?;
			connections.push(client);
		}
		let pool = tangram_pool::Pool::new(connections);
		let database = Self { pool };
		Ok(database)
	}

	pub async fn connection(&self) -> Result<tangram_pool::Guard<Connection>> {
		let connection = self.pool.get().await;
		Ok(connection)
	}
}

impl Connection {
	#[allow(clippy::unnecessary_wraps)]
	pub async fn connect(path: &Path) -> Result<Self> {
		let path = path.to_owned();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		tokio::task::spawn_blocking(move || Self::run(path, sender));
		let sender = receiver.await.unwrap()?;
		Ok(Self { sender })
	}

	pub async fn execute(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<u64> {
		let statement = statement.into().into_owned();
		let params = params.into_iter().collect();
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

	pub async fn query(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Row>>> {
		let statement = statement.into().into_owned();
		let params = params.into_iter().collect();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::Query(QueryMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let sender = receiver.await.unwrap()?;
		let stream = stream::try_unfold(sender, |sender| async move {
			let (sender_, receiver) = tokio::sync::oneshot::channel();
			sender
				.send(sender_)
				.map_err(|source| error!(!source, "failed to get the next row"))?;
			let result = receiver
				.await
				.map_err(|source| error!(!source, "failed to get the next row"))?;
			match result {
				Ok(Some(row)) => Ok(Some((row, sender))),
				Ok(None) => Ok(None),
				Err(error) => Err(error),
			}
		});
		Ok(stream)
	}

	pub async fn query_optional(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Row>> {
		let rows = self.query_all(statement, params).await?;
		let option = rows.into_iter().next();
		Ok(option)
	}

	pub async fn query_one(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Row> {
		let rows = self.query_all(statement, params).await?;
		let row = rows
			.into_iter()
			.next()
			.ok_or_else(|| error!("expected a row"))?;
		Ok(row)
	}

	pub async fn query_all(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Row>> {
		let statement = statement.into().into_owned();
		let params = params.into_iter().collect();
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

	pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender
			.send(ConnectionMessage::Transaction(sender))
			.unwrap();
		let sender = receiver.await.unwrap()?;
		Ok(Transaction {
			sender,
			_marker: PhantomData,
		})
	}

	pub async fn with<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut sqlite::Connection) -> Result<R> + Send + Sync + 'static,
		R: Send + Sync + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender
			.send(ConnectionMessage::With(Box::new(|connection| {
				sender.send(f(connection)).map_err(|_| ()).unwrap();
			})))
			.unwrap();
		receiver.await.unwrap()
	}

	fn run(
		path: PathBuf,
		sender: tokio::sync::oneshot::Sender<
			Result<tokio::sync::mpsc::UnboundedSender<ConnectionMessage>>,
		>,
	) {
		let connection = || {
			let connection = sqlite::Connection::open(path)?;
			connection.pragma_update(None, "busy_timeout", "86400000")?;
			Ok::<_, sqlite::Error>(connection)
		};
		let mut connection = match connection() {
			Ok(connection) => connection,
			Err(source) => {
				let error = error!(!source, "failed to open the database");
				sender.send(Err(error)).ok();
				return;
			},
		};
		let (sender_, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		sender.send(Ok(sender_)).ok();
		while let Some(message) = receiver.blocking_recv() {
			match message {
				ConnectionMessage::Execute(message) => {
					execute(&connection, message);
				},
				ConnectionMessage::Query(message) => {
					query(&connection, message);
				},
				ConnectionMessage::QueryAll(message) => {
					query_all(&connection, message);
				},
				ConnectionMessage::Transaction(sender) => {
					Transaction::run(&mut connection, sender);
				},
				ConnectionMessage::With(f) => {
					f(&mut connection);
				},
			}
		}
	}
}

impl<'a> Transaction<'a> {
	pub async fn execute(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<u64> {
		let statement = statement.into().into_owned();
		let params = params.into_iter().collect();
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

	pub async fn query(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Row>>> {
		let statement = statement.into().into_owned();
		let params = params.into_iter().collect();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::Query(QueryMessage {
			statement,
			params,
			sender,
		});
		self.sender.send(message).unwrap();
		let sender = receiver.await.unwrap()?;
		let stream = stream::try_unfold(sender, |sender| async move {
			let (sender_, receiver) = tokio::sync::oneshot::channel();
			sender
				.send(sender_)
				.map_err(|source| error!(!source, "failed to get the next row"))?;
			let result = receiver
				.await
				.map_err(|source| error!(!source, "failed to get the next row"))?;
			match result {
				Ok(Some(row)) => Ok(Some((row, sender))),
				Ok(None) => Ok(None),
				Err(error) => Err(error),
			}
		});
		Ok(stream)
	}

	pub async fn query_optional(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Row>> {
		let rows = self.query_all(statement, params).await?;
		let option = rows.into_iter().next();
		Ok(option)
	}

	pub async fn query_one(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Row> {
		let rows = self.query_all(statement, params).await?;
		let row = rows
			.into_iter()
			.next()
			.ok_or_else(|| error!("expected a row"))?;
		Ok(row)
	}

	pub async fn query_all(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Row>> {
		let statement = statement.into().into_owned();
		let params = params.into_iter().collect();
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

	pub async fn rollback(self) -> Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender
			.send(TransactionMessage::Rollback(sender))
			.unwrap();
		receiver.await.unwrap()?;
		Ok(())
	}

	pub async fn commit(self) -> Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender
			.send(TransactionMessage::Commit(sender))
			.unwrap();
		receiver.await.unwrap()?;
		Ok(())
	}

	pub async fn with<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut sqlite::Transaction) -> Result<R> + Send + Sync + 'static,
		R: Send + Sync + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender
			.send(TransactionMessage::With(Box::new(|transaction| {
				sender.send(f(transaction)).map_err(|_| ()).unwrap();
			})))
			.unwrap();
		receiver.await.unwrap()
	}

	fn run(
		connection: &mut sqlite::Connection,
		sender: tokio::sync::oneshot::Sender<
			Result<tokio::sync::mpsc::UnboundedSender<TransactionMessage>>,
		>,
	) {
		let mut transaction = match connection.transaction() {
			Ok(transaction) => transaction,
			Err(error) => {
				let error = error!(source = error, "failed to begin the transaction");
				sender.send(Err(error)).ok();
				return;
			},
		};
		let (sender_, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		sender.send(Ok(sender_)).ok();
		while let Some(message) = receiver.blocking_recv() {
			match message {
				TransactionMessage::Execute(message) => {
					execute(&transaction, message);
				},
				TransactionMessage::Query(message) => {
					query(&transaction, message);
				},
				TransactionMessage::QueryAll(message) => {
					query_all(&transaction, message);
				},
				TransactionMessage::Commit(sender) => {
					let result = transaction.commit().map_err(|error| {
						error!(source = error, "failed to commit the transaction")
					});
					sender.send(result).ok();
					return;
				},
				TransactionMessage::Rollback(sender) => {
					let result = transaction.rollback().map_err(|error| {
						error!(source = error, "failed to roll back the transaction")
					});
					sender.send(result).ok();
					return;
				},
				TransactionMessage::With(f) => {
					f(&mut transaction);
				},
			}
		}
	}
}

fn execute(connection: &sqlite::Connection, message: ExecuteMessage) {
	let ExecuteMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match connection.prepare_cached(&statement) {
		Ok(statement) => statement,
		Err(source) => {
			let error = error!(!source, "failed to prepare the statement");
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Execute the statement.
	let n = match statement.execute(sqlite::params_from_iter(params)) {
		Ok(n) => n.to_u64().unwrap(),
		Err(source) => {
			let error = error!(!source, "failed to prepare the statement");
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Send the result.
	sender.send(Ok(n)).ok();
}

fn query(connection: &sqlite::Connection, message: QueryMessage) {
	let QueryMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match connection.prepare_cached(&statement) {
		Ok(statement) => statement,
		Err(source) => {
			let error = error!(!source, "failed to prepare the statement");
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Get the column names.
	let column_names = statement
		.column_names()
		.into_iter()
		.map(ToOwned::to_owned)
		.collect_vec();

	// Execute the statement.
	let mut rows = match statement.query(sqlite::params_from_iter(params)) {
		Ok(rows) => rows,
		Err(source) => {
			let error = error!(!source, "failed to prepare the statement");
			sender.send(Err(error)).ok();
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
			Err(source) => {
				let error = error!(!source, "failed to get the row");
				sender.send(Err(error)).ok();
				continue;
			},
		};
		let mut entries = IndexMap::with_capacity(column_names.len());
		for (i, column) in column_names.iter().enumerate() {
			let name = column.to_owned();
			let value = match row.get::<_, Value>(i) {
				Ok(value) => value,
				Err(source) => {
					let error = error!(!source, "failed to get the column");
					sender.send(Err(error)).ok();
					continue 'a;
				},
			};
			entries.insert(name, value);
		}
		let row = Row::with_entries(entries);
		sender.send(Ok(Some(row))).ok();
	}
}

fn query_all(connection: &sqlite::Connection, message: QueryAllMessage) {
	let QueryAllMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match connection.prepare_cached(&statement) {
		Ok(statement) => statement,
		Err(source) => {
			let error = error!(!source, "failed to prepare the statement");
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Get the column names.
	let column_names = statement
		.column_names()
		.into_iter()
		.map(ToOwned::to_owned)
		.collect_vec();

	// Execute the statement.
	let rows = match statement.query(sqlite::params_from_iter(params)) {
		Ok(rows) => rows,
		Err(source) => {
			let error = error!(!source, "failed to prepare the statement");
			sender.send(Err(error)).ok();
			return;
		},
	};

	// Collect the rows.
	let result = rows
		.and_then(|row| {
			let mut entries = IndexMap::with_capacity(column_names.len());
			for (i, column) in column_names.iter().enumerate() {
				let name = column.to_owned();
				let value = row.get::<_, Value>(i)?;
				entries.insert(name, value);
			}
			let row = Row::with_entries(entries);
			Ok::<_, sqlite::Error>(row)
		})
		.try_collect()
		.map_err(|source| error!(!source, "failed to collect the rows"));

	// Send the result.
	sender.send(result).ok();
}

impl sqlite::types::ToSql for Value {
	fn to_sql(&self) -> sqlite::Result<sqlite::types::ToSqlOutput<'_>> {
		match self {
			Value::Null => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Null,
			)),
			Value::Integer(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Integer(*value),
			)),
			Value::Real(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Real(*value),
			)),
			Value::Text(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Text(value.as_bytes()),
			)),
			Value::Blob(value) => Ok(sqlite::types::ToSqlOutput::Borrowed(
				sqlite::types::ValueRef::Blob(value.as_ref()),
			)),
		}
	}
}

impl sqlite::types::FromSql for Value {
	fn column_result(value: sqlite::types::ValueRef<'_>) -> sqlite::types::FromSqlResult<Self> {
		match value {
			sqlite::types::ValueRef::Null => Ok(Self::Null),
			sqlite::types::ValueRef::Integer(value) => Ok(Self::Integer(value)),
			sqlite::types::ValueRef::Real(value) => Ok(Self::Real(value)),
			sqlite::types::ValueRef::Text(value) => Ok(Self::Text(
				String::from_utf8(value.to_owned())
					.map_err(|error| sqlite::types::FromSqlError::Other(error.into()))?,
			)),
			sqlite::types::ValueRef::Blob(value) => Ok(Self::Blob(value.to_owned())),
		}
	}
}

impl From<sqlite::types::Value> for Value {
	fn from(value: sqlite::types::Value) -> Self {
		match value {
			sqlite::types::Value::Null => Self::Null,
			sqlite::types::Value::Integer(value) => Self::Integer(value),
			sqlite::types::Value::Real(value) => Self::Real(value),
			sqlite::types::Value::Text(value) => Self::Text(value),
			sqlite::types::Value::Blob(value) => Self::Blob(value),
		}
	}
}

impl<'a> From<sqlite::types::ValueRef<'a>> for Value {
	fn from(value: sqlite::types::ValueRef<'a>) -> Self {
		match value {
			sqlite::types::ValueRef::Null => Self::Null,
			sqlite::types::ValueRef::Integer(value) => Self::Integer(value),
			sqlite::types::ValueRef::Real(value) => Self::Real(value),
			sqlite::types::ValueRef::Text(value) => {
				Self::Text(String::from_utf8(value.to_owned()).unwrap())
			},
			sqlite::types::ValueRef::Blob(value) => Self::Blob(value.to_owned()),
		}
	}
}

impl From<Value> for sqlite::types::Value {
	fn from(value: Value) -> Self {
		match value {
			Value::Null => Self::Null,
			Value::Integer(value) => Self::Integer(value),
			Value::Real(value) => Self::Real(value),
			Value::Text(value) => Self::Text(value),
			Value::Blob(value) => Self::Blob(value),
		}
	}
}

impl<T> sqlite::types::ToSql for Json<T>
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

impl<T> sqlite::types::FromSql for Json<T>
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

#[allow(clippy::module_name_repetitions)]
#[macro_export]
macro_rules! sqlite_params {
	($($x:expr),* $(,)?) => {
		&[$(&$x as &(dyn rusqlite::types::ToSql),)*] as &[&(dyn rusqlite::types::ToSql)]
	};
}

pub use sqlite_params as params;
