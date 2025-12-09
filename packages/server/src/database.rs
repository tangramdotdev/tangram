use {
	futures::{Stream, StreamExt as _},
	std::borrow::Cow,
	tangram_database as db,
};

pub mod sqlite;

#[derive(
	Debug,
	derive_more::IsVariant,
	derive_more::Display,
	derive_more::Error,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Error {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Error),
	Sqlite(db::sqlite::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Database {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Database),
	Sqlite(db::sqlite::Database),
}

#[expect(dead_code)]
#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum DatabaseOptions {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::DatabaseOptions),
	Sqlite(db::sqlite::DatabaseOptions),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Connection {
	#[cfg(feature = "postgres")]
	Postgres(db::pool::Guard<db::postgres::Connection>),
	Sqlite(db::pool::Guard<db::sqlite::Connection>),
}

#[expect(dead_code)]
#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum ConnectionOptions {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::ConnectionOptions),
	Sqlite(db::sqlite::ConnectionOptions),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Transaction<'a> {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Transaction<'a>),
	Sqlite(db::sqlite::Transaction<'a>),
}

#[cfg(feature = "postgres")]
impl From<db::postgres::Error> for Error {
	fn from(error: db::postgres::Error) -> Self {
		Self::Postgres(error)
	}
}

impl From<db::sqlite::Error> for Error {
	fn from(error: db::sqlite::Error) -> Self {
		Self::Sqlite(error)
	}
}

impl db::Error for Error {
	fn is_retry(&self) -> bool {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(e) => e.is_retry(),
			Self::Sqlite(e) => e.is_retry(),
			Self::Other(_) => false,
		}
	}

	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl db::Database for Database {
	type Error = Error;

	type Connection = Connection;

	async fn connection_with_options(
		&self,
		options: db::ConnectionOptions,
	) -> Result<Self::Connection, Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let connection = s
					.connection_with_options(options)
					.await
					.map_err(Error::Postgres)?;
				Ok(Connection::Postgres(connection))
			},
			Self::Sqlite(s) => {
				let connection = s
					.connection_with_options(options)
					.await
					.map_err(Error::Sqlite)?;
				Ok(Connection::Sqlite(connection))
			},
		}
	}

	async fn sync(&self) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.sync().await.map_err(Error::Postgres),
			Self::Sqlite(s) => s.sync().await.map_err(Error::Sqlite),
		}
	}
}

impl db::Connection for Connection {
	type Error = Error;

	type Transaction<'a> = Transaction<'a>;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let transaction = s.transaction().await.map_err(Error::Postgres)?;
				Ok(Transaction::Postgres(transaction))
			},
			Self::Sqlite(s) => {
				let transaction = s.transaction().await.map_err(Error::Sqlite)?;
				Ok(Transaction::Sqlite(transaction))
			},
		}
	}
}

impl db::Transaction for Transaction<'_> {
	type Error = Error;

	async fn rollback(self) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.rollback().await.map_err(Error::Postgres),
			Self::Sqlite(s) => s.rollback().await.map_err(Error::Sqlite),
		}
	}

	async fn commit(self) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.commit().await.map_err(Error::Postgres),
			Self::Sqlite(s) => s.commit().await.map_err(Error::Sqlite),
		}
	}
}

impl db::Query for Connection {
	type Error = Error;

	fn p(&self) -> &'static str {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.p(),
			Self::Sqlite(s) => s.p(),
		}
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<u64, Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.execute(statement, params).await.map_err(Error::Postgres),
			Self::Sqlite(s) => s.execute(statement, params).await.map_err(Error::Sqlite),
		}
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Postgres)?;
				Ok(stream.map(|result| result.map_err(Error::Postgres)).boxed())
			},
			Self::Sqlite(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Sqlite)?;
				Ok(stream.map(|result| result.map_err(Error::Sqlite)).boxed())
			},
		}
	}
}

impl db::Query for Transaction<'_> {
	type Error = Error;

	fn p(&self) -> &'static str {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.p(),
			Self::Sqlite(s) => s.p(),
		}
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<u64, Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.execute(statement, params).await.map_err(Error::Postgres),
			Self::Sqlite(s) => s.execute(statement, params).await.map_err(Error::Sqlite),
		}
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row, Self::Error>> + Send, Self::Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Postgres)?;
				Ok(stream.map(|result| result.map_err(Error::Postgres)).boxed())
			},
			Self::Sqlite(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Sqlite)?;
				Ok(stream.map(|result| result.map_err(Error::Sqlite)).boxed())
			},
		}
	}
}
