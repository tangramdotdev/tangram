use futures::{Stream, StreamExt as _, TryStreamExt as _};
use tangram_database as db;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Sqlite(db::sqlite::Error),
	Postgres(db::postgres::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug)]
pub enum Options {
	Sqlite(db::sqlite::Options),
	Postgres(db::postgres::Options),
}

pub enum Database {
	Sqlite(db::sqlite::Database),
	Postgres(db::postgres::Database),
}

pub enum Connection<'a> {
	Sqlite(db::sqlite::Connection<'a>),
	Postgres(db::postgres::Connection<'a>),
}

pub enum Transaction<'a> {
	Sqlite(db::sqlite::Transaction<'a>),
	Postgres(db::postgres::Transaction<'a>),
}

impl Database {
	pub async fn new(options: Options) -> Result<Self, Error> {
		match options {
			Options::Sqlite(options) => {
				let database = db::sqlite::Database::new(options).await?;
				Ok(Self::Sqlite(database))
			},
			Options::Postgres(options) => {
				let database = db::postgres::Database::new(options).await?;
				Ok(Self::Postgres(database))
			},
		}
	}

	#[must_use]
	pub fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(_) => "?",
			Self::Postgres(_) => "$",
		}
	}
}

impl<'a> Connection<'a> {
	#[must_use]
	pub fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(_) => "?",
			Self::Postgres(_) => "$",
		}
	}
}

impl<'a> Transaction<'a> {
	#[must_use]
	pub fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(_) => "?",
			Self::Postgres(_) => "$",
		}
	}
}

impl db::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl db::Database for Database {
	type Error = Error;

	type Connection<'c> = Connection<'c> where Self: 'c;

	async fn connection(&self) -> Result<Self::Connection<'_>, Self::Error> {
		match self {
			Self::Sqlite(database) => Ok(Connection::Sqlite(database.connection().await?)),
			Self::Postgres(database) => Ok(Connection::Postgres(database.connection().await?)),
		}
	}
}

impl<'a> db::Connection for Connection<'a> {
	type Error = Error;

	type Transaction<'t> = Transaction<'t> where Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		match self {
			Self::Sqlite(connection) => Ok(Transaction::Sqlite(connection.transaction().await?)),
			Self::Postgres(connection) => {
				Ok(Transaction::Postgres(connection.transaction().await?))
			},
		}
	}
}

impl<'a> db::Transaction for Transaction<'a> {
	type Error = Error;

	async fn rollback(self) -> Result<(), Self::Error> {
		match self {
			Transaction::Sqlite(transaction) => transaction.rollback().await.map_err(Into::into),
			Transaction::Postgres(transaction) => transaction.rollback().await.map_err(Into::into),
		}
	}

	async fn commit(self) -> Result<(), Self::Error> {
		match self {
			Transaction::Sqlite(transaction) => transaction.commit().await.map_err(Into::into),
			Transaction::Postgres(transaction) => transaction.commit().await.map_err(Into::into),
		}
	}
}

impl db::Query for Database {
	type Error = Error;

	async fn execute(&self, statement: String, params: Vec<db::Value>) -> Result<u64, Self::Error> {
		match self {
			Self::Sqlite(database) => database
				.execute(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(database) => database
				.execute(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		let stream = match self {
			Self::Sqlite(database) => database
				.query(statement, params)
				.await
				.map_err(Error::from)?
				.map_err(Into::into)
				.left_stream(),
			Self::Postgres(database) => database
				.query(statement, params)
				.await
				.map_err(Error::from)?
				.map_err(Into::into)
				.right_stream(),
		};
		Ok(stream)
	}

	async fn query_optional(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Option<db::Row>, Self::Error> {
		match self {
			Self::Sqlite(database) => database
				.query_optional(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(database) => database
				.query_optional(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query_one(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<db::Row, Self::Error> {
		match self {
			Self::Sqlite(database) => database
				.query_one(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(database) => database
				.query_one(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query_all(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Vec<db::Row>, Self::Error> {
		match self {
			Self::Sqlite(database) => database
				.query_all(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(database) => database
				.query_all(statement, params)
				.await
				.map_err(Into::into),
		}
	}
}

impl<'c> db::Query for Connection<'c> {
	type Error = Error;

	async fn execute(&self, statement: String, params: Vec<db::Value>) -> Result<u64, Self::Error> {
		match self {
			Self::Sqlite(connection) => connection
				.execute(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(connection) => connection
				.execute(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		let stream = match self {
			Self::Sqlite(connection) => connection
				.query(statement, params)
				.await
				.map_err(Error::from)?
				.map_err(Into::into)
				.left_stream(),
			Self::Postgres(connection) => connection
				.query(statement, params)
				.await
				.map_err(Error::from)?
				.map_err(Into::into)
				.right_stream(),
		};
		Ok(stream)
	}

	async fn query_optional(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Option<db::Row>, Self::Error> {
		match self {
			Self::Sqlite(connection) => connection
				.query_optional(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(connection) => connection
				.query_optional(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query_one(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<db::Row, Self::Error> {
		match self {
			Self::Sqlite(connection) => connection
				.query_one(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(connection) => connection
				.query_one(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query_all(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Vec<db::Row>, Self::Error> {
		match self {
			Self::Sqlite(connection) => connection
				.query_all(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(connection) => connection
				.query_all(statement, params)
				.await
				.map_err(Into::into),
		}
	}
}

impl<'t> db::Query for Transaction<'t> {
	type Error = Error;

	async fn execute(&self, statement: String, params: Vec<db::Value>) -> Result<u64, Self::Error> {
		match self {
			Self::Sqlite(transaction) => transaction
				.execute(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(transaction) => transaction
				.execute(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		let stream = match self {
			Self::Sqlite(transaction) => transaction
				.query(statement, params)
				.await
				.map_err(Error::from)?
				.map_err(Into::into)
				.left_stream(),
			Self::Postgres(transaction) => transaction
				.query(statement, params)
				.await
				.map_err(Error::from)?
				.map_err(Into::into)
				.right_stream(),
		};
		Ok(stream)
	}

	async fn query_optional(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Option<db::Row>, Self::Error> {
		match self {
			Self::Sqlite(transaction) => transaction
				.query_optional(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(transaction) => transaction
				.query_optional(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query_one(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<db::Row, Self::Error> {
		match self {
			Self::Sqlite(transaction) => transaction
				.query_one(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(transaction) => transaction
				.query_one(statement, params)
				.await
				.map_err(Into::into),
		}
	}

	async fn query_all(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Vec<db::Row>, Self::Error> {
		match self {
			Self::Sqlite(transaction) => transaction
				.query_all(statement, params)
				.await
				.map_err(Into::into),
			Self::Postgres(transaction) => transaction
				.query_all(statement, params)
				.await
				.map_err(Into::into),
		}
	}
}
