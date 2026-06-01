use {
	futures::{Stream, StreamExt as _},
	std::borrow::Cow,
	tangram_client as tg,
	tangram_database::{self as db},
};

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "turso")]
pub mod turso;

mod run;

mod retry;

pub(crate) use {retry::retry, run::run};

#[derive(
	Debug,
	derive_more::Display,
	derive_more::Error,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[unwrap(ref)]
pub enum Error {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Error),

	#[cfg(feature = "sqlite")]
	Sqlite(db::sqlite::Error),

	#[cfg(feature = "turso")]
	Turso(db::turso::Error),

	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Database {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Database),

	#[cfg(feature = "sqlite")]
	Sqlite(db::sqlite::Database),

	#[cfg(feature = "turso")]
	Turso(db::turso::Database),
}

#[expect(dead_code)]
#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum DatabaseOptions {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::DatabaseOptions),

	#[cfg(feature = "sqlite")]
	Sqlite(db::sqlite::DatabaseOptions),

	#[cfg(feature = "turso")]
	Turso(db::turso::DatabaseOptions),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Connection {
	#[cfg(feature = "postgres")]
	Postgres(tangram_pool::ExclusiveGuard<db::postgres::Connection, db::postgres::Error>),

	#[cfg(feature = "sqlite")]
	Sqlite(tangram_pool::ExclusiveGuard<db::sqlite::Connection, db::sqlite::Error>),

	#[cfg(feature = "turso")]
	Turso(tangram_pool::ExclusiveGuard<db::turso::Connection, db::turso::Error>),
}

#[expect(dead_code)]
#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum ConnectionOptions {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::ConnectionOptions),

	#[cfg(feature = "sqlite")]
	Sqlite(db::sqlite::ConnectionOptions),
}

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Transaction<'a> {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Transaction<'a>),

	#[cfg(feature = "sqlite")]
	Sqlite(db::sqlite::Transaction<'a>),

	#[cfg(feature = "turso")]
	Turso(db::turso::Transaction<'a>),
}

impl db::Error for Error {
	fn is_retry(&self) -> bool {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(e) => e.is_retry(),
			#[cfg(feature = "sqlite")]
			Self::Sqlite(e) => e.is_retry(),
			#[cfg(feature = "turso")]
			Self::Turso(e) => e.is_retry(),
			Self::Other(_) => false,
		}
	}

	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl From<tg::Error> for Error {
	fn from(error: tg::Error) -> Self {
		Self::Other(Box::new(error))
	}
}

impl db::Database for Database {
	type Error = Error;

	type Connection = Connection;

	async fn connection_with_options(
		&self,
		options: db::ConnectionOptions,
	) -> std::result::Result<Self::Connection, Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let connection = s
					.connection_with_options(options)
					.await
					.map_err(Error::Postgres)?;
				Ok(Connection::Postgres(connection))
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => {
				let connection = s
					.connection_with_options(options)
					.await
					.map_err(Error::Sqlite)?;
				Ok(Connection::Sqlite(connection))
			},
			#[cfg(feature = "turso")]
			Self::Turso(s) => {
				let connection = s
					.connection_with_options(options)
					.await
					.map_err(Error::Turso)?;
				Ok(Connection::Turso(connection))
			},
		}
	}

	async fn sync(&self) -> std::result::Result<(), Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.sync().await.map_err(Error::Postgres),
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => s.sync().await.map_err(Error::Sqlite),
			#[cfg(feature = "turso")]
			Self::Turso(s) => s.sync().await.map_err(Error::Turso),
		}
	}
}

impl db::Connection for Connection {
	type Error = Error;

	type Transaction<'a> = Transaction<'a>;

	async fn transaction(&mut self) -> std::result::Result<Self::Transaction<'_>, Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let transaction = s.transaction().await.map_err(Error::Postgres)?;
				Ok(Transaction::Postgres(transaction))
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => {
				let transaction = s.transaction().await.map_err(Error::Sqlite)?;
				Ok(Transaction::Sqlite(transaction))
			},
			#[cfg(feature = "turso")]
			Self::Turso(s) => {
				let transaction = s.transaction().await.map_err(Error::Turso)?;
				Ok(Transaction::Turso(transaction))
			},
		}
	}
}

impl db::Transaction for Transaction<'_> {
	type Error = Error;

	async fn rollback(self) -> std::result::Result<(), Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.rollback().await.map_err(Error::Postgres),
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => s.rollback().await.map_err(Error::Sqlite),
			#[cfg(feature = "turso")]
			Self::Turso(s) => s.rollback().await.map_err(Error::Turso),
		}
	}

	async fn commit(self) -> std::result::Result<(), Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.commit().await.map_err(Error::Postgres),
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => s.commit().await.map_err(Error::Sqlite),
			#[cfg(feature = "turso")]
			Self::Turso(s) => s.commit().await.map_err(Error::Turso),
		}
	}
}

impl db::Query for Connection {
	type Error = Error;

	fn p(&self) -> &'static str {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.p(),
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => s.p(),
			#[cfg(feature = "turso")]
			Self::Turso(s) => s.p(),
		}
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> std::result::Result<u64, Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.execute(statement, params).await.map_err(Error::Postgres),
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => s.execute(statement, params).await.map_err(Error::Sqlite),
			#[cfg(feature = "turso")]
			Self::Turso(s) => s.execute(statement, params).await.map_err(Error::Turso),
		}
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> std::result::Result<impl Stream<Item = std::result::Result<db::Row, Error>> + Send, Error>
	{
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Postgres)?;
				Ok(stream.map(|result| result.map_err(Error::Postgres)).boxed())
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Sqlite)?;
				Ok(stream.map(|result| result.map_err(Error::Sqlite)).boxed())
			},
			#[cfg(feature = "turso")]
			Self::Turso(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Turso)?;
				Ok(stream.map(|result| result.map_err(Error::Turso)).boxed())
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
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => s.p(),
			#[cfg(feature = "turso")]
			Self::Turso(s) => s.p(),
		}
	}

	async fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> std::result::Result<u64, Error> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => s.execute(statement, params).await.map_err(Error::Postgres),
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => s.execute(statement, params).await.map_err(Error::Sqlite),
			#[cfg(feature = "turso")]
			Self::Turso(s) => s.execute(statement, params).await.map_err(Error::Turso),
		}
	}

	async fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<db::Value>,
	) -> std::result::Result<impl Stream<Item = std::result::Result<db::Row, Error>> + Send, Error>
	{
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Postgres)?;
				Ok(stream.map(|result| result.map_err(Error::Postgres)).boxed())
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Sqlite)?;
				Ok(stream.map(|result| result.map_err(Error::Sqlite)).boxed())
			},
			#[cfg(feature = "turso")]
			Self::Turso(s) => {
				let stream = s.query(statement, params).await.map_err(Error::Turso)?;
				Ok(stream.map(|result| result.map_err(Error::Turso)).boxed())
			},
		}
	}
}
