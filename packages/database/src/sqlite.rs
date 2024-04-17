use crate::{Error as _, Row, Value};
use futures::{stream, Stream};
use indexmap::IndexMap;
use itertools::Itertools as _;
use num::ToPrimitive;
use rusqlite as sqlite;
use std::path::PathBuf;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Sqlite(sqlite::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug)]
pub struct Options {
	pub path: PathBuf,
	pub max_connections: usize,
}

pub struct Database {
	sender: async_channel::Sender<DatabaseMessage>,
}

pub struct Connection<'a> {
	marker: std::marker::PhantomData<&'a ()>,
	sender: tokio::sync::mpsc::UnboundedSender<ConnectionMessage>,
}

pub struct Transaction<'a> {
	marker: std::marker::PhantomData<&'a ()>,
	sender: tokio::sync::mpsc::UnboundedSender<TransactionMessage>,
}

enum DatabaseMessage {
	Execute(ExecuteMessage),
	Query(QueryMessage),
	QueryAll(QueryAllMessage),
	Connection(DatabaseConnectionMessage),
	With(DatabaseWithMessage),
}

type DatabaseWithMessage = Box<dyn FnOnce(&mut sqlite::Connection) + Send>;

struct DatabaseConnectionMessage {
	sender: tokio::sync::oneshot::Sender<
		Result<tokio::sync::mpsc::UnboundedSender<ConnectionMessage>, Error>,
	>,
}

enum ConnectionMessage {
	Execute(ExecuteMessage),
	Query(QueryMessage),
	QueryAll(QueryAllMessage),
	Transaction(ConnectionTransactionMessage),
	With(ConnectionWithMessage),
}

type ConnectionWithMessage = Box<dyn FnOnce(&mut sqlite::Connection) + Send>;

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

type TransactionWithMessage = Box<dyn FnOnce(&mut sqlite::Transaction) + Send>;

struct TransactionRollbackMessage {
	sender: tokio::sync::oneshot::Sender<Result<(), Error>>,
}

struct TransactionCommitMessage {
	sender: tokio::sync::oneshot::Sender<Result<(), Error>>,
}

struct ExecuteMessage {
	statement: String,
	params: Vec<Value>,
	sender: tokio::sync::oneshot::Sender<Result<u64, Error>>,
}

struct QueryMessage {
	statement: String,
	params: Vec<Value>,
	sender: QueryMessageSender,
}

type QueryMessageSender = tokio::sync::oneshot::Sender<Result<QueryMessageRowSender, Error>>;

type QueryMessageRowSender =
	tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<Result<Option<Row>, Error>>>;

struct QueryAllMessage {
	statement: String,
	params: Vec<Value>,
	sender: tokio::sync::oneshot::Sender<Result<Vec<Row>, Error>>,
}

pub struct Json<T>(pub T);

impl Database {
	pub async fn new(options: Options) -> Result<Self, Error> {
		let (sender, receiver) = async_channel::unbounded::<DatabaseMessage>();
		for _ in 0..options.max_connections {
			let receiver = receiver.clone();
			let mut connection = sqlite::Connection::open(&options.path)?;
			connection.pragma_update(None, "busy_timeout", "86400000")?;
			tokio::task::spawn_blocking(move || {
				while let Ok(message) = receiver.recv_blocking() {
					match message {
						DatabaseMessage::Execute(message) => {
							handle_execute_message(&connection, message);
						},
						DatabaseMessage::Query(message) => {
							handle_query_message(&connection, message);
						},
						DatabaseMessage::QueryAll(message) => {
							handle_query_all_message(&connection, message);
						},
						DatabaseMessage::Connection(message) => {
							handle_database_connection_message(&mut connection, message);
						},
						DatabaseMessage::With(f) => {
							f(&mut connection);
						},
					}
				}
			});
		}
		Ok(Self { sender })
	}

	pub async fn with<F, T, E>(&self, f: F) -> Result<T, E>
	where
		F: FnOnce(&mut sqlite::Connection) -> Result<T, E> + Send + 'static,
		T: Send + 'static,
		E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = DatabaseMessage::With(Box::new(|connection| {
			sender.send(f(connection)).map_err(|_| ()).unwrap();
		}));
		self.sender.try_send(message).unwrap();
		receiver.await.unwrap()
	}
}

impl<'a> Connection<'a> {
	pub async fn with<F, T, E>(&self, f: F) -> Result<T, E>
	where
		F: FnOnce(&mut sqlite::Connection) -> Result<T, E> + Send + 'static,
		T: Send + 'static,
		E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::With(Box::new(|connection| {
			sender.send(f(connection)).map_err(|_| ()).unwrap();
		}));
		self.sender.send(message).unwrap();
		receiver.await.unwrap()
	}
}

impl<'a> Transaction<'a> {
	pub async fn with<F, T, E>(&self, f: F) -> Result<T, E>
	where
		F: FnOnce(&mut sqlite::Transaction) -> Result<T, E> + Send + 'static,
		T: Send + 'static,
		E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = TransactionMessage::With(Box::new(|connection| {
			sender.send(f(connection)).map_err(|_| ()).unwrap();
		}));
		self.sender.send(message).unwrap();
		receiver.await.unwrap()
	}
}

impl super::Database for Database {
	type Error = Error;

	type Connection<'c> = Connection<'c> where Self: 'c;

	async fn connection(&self) -> Result<Self::Connection<'_>, Self::Error> {
		let marker = std::marker::PhantomData;
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = DatabaseMessage::Connection(DatabaseConnectionMessage { sender });
		self.sender.try_send(message).unwrap();
		let sender = receiver.await.unwrap()?;
		Ok(Connection { marker, sender })
	}
}

impl<'a> super::Connection for Connection<'a> {
	type Error = Error;

	type Transaction<'t> = Transaction<'t> where Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		let marker = std::marker::PhantomData;
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = ConnectionMessage::Transaction(ConnectionTransactionMessage { sender });
		self.sender.send(message).unwrap();
		let sender = receiver.await.unwrap()?;
		Ok(Transaction { marker, sender })
	}
}

impl<'a> super::Transaction for Transaction<'a> {
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

impl super::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl super::Query for Database {
	type Error = Error;

	async fn execute(&self, statement: String, params: Vec<Value>) -> Result<u64, Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = DatabaseMessage::Execute(ExecuteMessage {
			statement,
			params,
			sender,
		});
		self.sender.try_send(message).unwrap();
		let n = receiver.await.unwrap()?;
		Ok(n)
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = DatabaseMessage::Query(QueryMessage {
			statement,
			params,
			sender,
		});
		self.sender.try_send(message).unwrap();
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
		statement: String,
		params: Vec<Value>,
	) -> Result<Option<Row>, Self::Error> {
		Ok(self.query_all(statement, params).await?.into_iter().next())
	}

	async fn query_one(&self, statement: String, params: Vec<Value>) -> Result<Row, Self::Error> {
		self.query_optional(statement, params)
			.await?
			.ok_or_else(|| Error::other("expected a row"))
	}

	async fn query_all(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> Result<Vec<Row>, Self::Error> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = DatabaseMessage::QueryAll(QueryAllMessage {
			statement,
			params,
			sender,
		});
		self.sender.try_send(message).unwrap();
		let rows = receiver.await.unwrap()?;
		Ok(rows)
	}
}

impl<'c> super::Query for Connection<'c> {
	type Error = Error;

	async fn execute<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
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

	async fn query<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
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

	async fn query_optional<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<Option<Row>, Self::Error> {
		Ok(self.query_all(statement, params).await?.into_iter().next())
	}

	async fn query_one<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<Row, Self::Error> {
		self.query_optional(statement, params)
			.await?
			.ok_or_else(|| Error::other("expected a row"))
	}

	async fn query_all<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<Vec<Row>, Self::Error> {
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

impl<'t> super::Query for Transaction<'t> {
	type Error = Error;

	async fn execute<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
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

	async fn query<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
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

	async fn query_optional<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<Option<Row>, Self::Error> {
		Ok(self.query_all(statement, params).await?.into_iter().next())
	}

	async fn query_one<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<Row, Self::Error> {
		self.query_optional(statement, params)
			.await?
			.ok_or_else(|| Error::other("expected a row"))
	}

	async fn query_all<'a>(
		&'a self,
		statement: String,
		params: Vec<Value>,
	) -> Result<Vec<Row>, Self::Error> {
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

fn handle_database_connection_message(
	connection: &mut sqlite::Connection,
	message: DatabaseConnectionMessage,
) {
	let DatabaseConnectionMessage { sender } = message;
	let (sender_, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ConnectionMessage>();
	sender.send(Ok(sender_)).ok();
	while let Some(message) = receiver.blocking_recv() {
		match message {
			ConnectionMessage::Execute(message) => {
				handle_execute_message(connection, message);
			},
			ConnectionMessage::Query(message) => {
				handle_query_message(connection, message);
			},
			ConnectionMessage::QueryAll(message) => {
				handle_query_all_message(connection, message);
			},
			ConnectionMessage::Transaction(message) => {
				handle_connection_transaction_message(connection, message);
			},
			ConnectionMessage::With(f) => {
				f(connection);
			},
		}
	}
}

fn handle_connection_transaction_message(
	connection: &mut sqlite::Connection,
	message: ConnectionTransactionMessage,
) {
	let ConnectionTransactionMessage { sender } = message;
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
				handle_execute_message(&transaction, message);
			},
			TransactionMessage::Query(message) => {
				handle_query_message(&transaction, message);
			},
			TransactionMessage::QueryAll(message) => {
				handle_query_all_message(&transaction, message);
			},
			TransactionMessage::Rollback(message) => {
				let result = transaction.rollback().map_err(Into::into);
				message.sender.send(result).ok();
				break;
			},
			TransactionMessage::Commit(message) => {
				let result = transaction.commit().map_err(Into::into);
				message.sender.send(result).ok();
				break;
			},
			TransactionMessage::With(f) => {
				f(&mut transaction);
			},
		}
	}
}

fn handle_execute_message(connection: &sqlite::Connection, message: ExecuteMessage) {
	let ExecuteMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match connection.prepare_cached(&statement) {
		Ok(statement) => statement,
		Err(error) => {
			sender.send(Err(error.into())).ok();
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

fn handle_query_message(connection: &sqlite::Connection, message: QueryMessage) {
	let QueryMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match connection.prepare_cached(&statement) {
		Ok(statement) => statement,
		Err(error) => {
			sender.send(Err(error.into())).ok();
			return;
		},
	};

	// Get the columns.
	let columns = statement
		.column_names()
		.into_iter()
		.map(ToOwned::to_owned)
		.collect_vec();

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
			let value = match row.get::<_, Value>(i) {
				Ok(value) => value,
				Err(error) => {
					sender.send(Err(error.into())).ok();
					continue 'a;
				},
			};
			entries.insert(name, value);
		}
		let row = Row::with_entries(entries);
		sender.send(Ok(Some(row))).ok();
	}
}

fn handle_query_all_message(connection: &sqlite::Connection, message: QueryAllMessage) {
	let QueryAllMessage {
		statement,
		params,
		sender,
	} = message;

	// Prepare the statement.
	let mut statement = match connection.prepare_cached(&statement) {
		Ok(statement) => statement,
		Err(error) => {
			sender.send(Err(error.into())).ok();
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
				let value = row.get::<_, Value>(i)?;
				entries.insert(name, value);
			}
			let row = Row::with_entries(entries);
			Ok::<_, sqlite::Error>(row)
		})
		.try_collect()
		.map_err(Into::into);

	// Send the result.
	sender.send(result).ok();
}

impl sqlite::types::ToSql for Value {
	fn to_sql(&self) -> sqlite::Result<sqlite::types::ToSqlOutput> {
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
	fn column_result(value: sqlite::types::ValueRef) -> sqlite::types::FromSqlResult<Self> {
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
	fn to_sql(&self) -> sqlite::Result<sqlite::types::ToSqlOutput> {
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
	fn column_result(value: sqlite::types::ValueRef) -> sqlite::types::FromSqlResult<Self> {
		let json = value.as_str()?;
		let value = serde_json::from_str(json)
			.map_err(|error| sqlite::types::FromSqlError::Other(error.into()))?;
		Ok(Self(value))
	}
}
