use futures::Stream;
use tangram_database as db;
use tangram_error::Result;

#[derive(Clone, Debug)]
pub enum Options {
	Sqlite(db::sqlite::Options),
	// Postgres(db::postgres::Options),
}

pub enum Database {
	Sqlite(db::sqlite::Database),
	// Postgres(db::postgres::Database),
}

pub enum Connection<'a> {
	Sqlite(db::sqlite::Connection<'a>),
	// Postgres(db::postgres::Connection),
}

pub enum Transaction<'a> {
	Sqlite(db::sqlite::Transaction<'a>),
	// Postgres(db::postgres::Transaction),
}

impl Database {
	pub async fn new(options: Options) -> Result<Self> {
		match options {
			Options::Sqlite(options) => {
				let database = db::sqlite::Database::new(options).await?;
				Ok(Self::Sqlite(database))
			},
			// Options::Postgres(options) => {
			// 	let database = db::postgres::Database::new(options).await?;
			// 	Ok(Self::Postgres(database))
			// },
		}
	}

	#[must_use]
	pub fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(_) => "?",
			// 	Self::Postgres(_) => "$",
		}
	}
}

impl<'a> Connection<'a> {
	#[must_use]
	pub fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(_) => "?",
			// 	Self::Postgres(_) => "$",
		}
	}
}

impl<'a> Transaction<'a> {
	#[must_use]
	pub fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(_) => "?",
			// 	Self::Postgres(_) => "$",
		}
	}
}

impl db::Database for Database {
	type Connection<'c> = Connection<'c> where Self: 'c;

	async fn connection(&self) -> Result<Self::Connection<'_>> {
		match self {
			Self::Sqlite(database) => Ok(Connection::Sqlite(database.connection().await?)),
		}
	}
}

impl<'a> db::Connection for Connection<'a> {
	type Transaction<'t> = Transaction<'t> where Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>> {
		match self {
			Self::Sqlite(connection) => Ok(Transaction::Sqlite(connection.transaction().await?)),
		}
	}
}

impl<'a> db::Transaction for Transaction<'a> {
	async fn rollback(self) -> Result<()> {
		match self {
			Transaction::Sqlite(transaction) => transaction.rollback().await,
		}
	}

	async fn commit(self) -> Result<()> {
		match self {
			Transaction::Sqlite(transaction) => transaction.commit().await,
		}
	}
}

impl db::Query for Database {
	async fn execute(&self, statement: String, params: Vec<db::Value>) -> Result<u64> {
		match self {
			Self::Sqlite(database) => database.execute(statement, params).await,
		}
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row>> + Send> {
		match self {
			Self::Sqlite(database) => database.query(statement, params).await,
		}
	}

	async fn query_optional(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Option<db::Row>> {
		match self {
			Self::Sqlite(database) => database.query_optional(statement, params).await,
		}
	}

	async fn query_one(&self, statement: String, params: Vec<db::Value>) -> Result<db::Row> {
		match self {
			Self::Sqlite(database) => database.query_one(statement, params).await,
		}
	}

	async fn query_all(&self, statement: String, params: Vec<db::Value>) -> Result<Vec<db::Row>> {
		match self {
			Self::Sqlite(database) => database.query_all(statement, params).await,
		}
	}
}

impl<'c> db::Query for Connection<'c> {
	async fn execute(&self, statement: String, params: Vec<db::Value>) -> Result<u64> {
		match self {
			Self::Sqlite(database) => database.execute(statement, params).await,
		}
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row>> + Send> {
		match self {
			Self::Sqlite(connection) => connection.query(statement, params).await,
		}
	}

	async fn query_optional(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Option<db::Row>> {
		match self {
			Self::Sqlite(database) => database.query_optional(statement, params).await,
		}
	}

	async fn query_one(&self, statement: String, params: Vec<db::Value>) -> Result<db::Row> {
		match self {
			Self::Sqlite(database) => database.query_one(statement, params).await,
		}
	}

	async fn query_all(&self, statement: String, params: Vec<db::Value>) -> Result<Vec<db::Row>> {
		match self {
			Self::Sqlite(database) => database.query_all(statement, params).await,
		}
	}
}

impl<'t> db::Query for Transaction<'t> {
	async fn execute(&self, statement: String, params: Vec<db::Value>) -> Result<u64> {
		match self {
			Self::Sqlite(database) => database.execute(statement, params).await,
		}
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<impl Stream<Item = Result<db::Row>> + Send> {
		match self {
			Self::Sqlite(transaction) => transaction.query(statement, params).await,
		}
	}

	async fn query_optional(
		&self,
		statement: String,
		params: Vec<db::Value>,
	) -> Result<Option<db::Row>> {
		match self {
			Self::Sqlite(database) => database.query_optional(statement, params).await,
		}
	}

	async fn query_one(&self, statement: String, params: Vec<db::Value>) -> Result<db::Row> {
		match self {
			Self::Sqlite(database) => database.query_one(statement, params).await,
		}
	}

	async fn query_all(&self, statement: String, params: Vec<db::Value>) -> Result<Vec<db::Row>> {
		match self {
			Self::Sqlite(database) => database.query_all(statement, params).await,
		}
	}
}
