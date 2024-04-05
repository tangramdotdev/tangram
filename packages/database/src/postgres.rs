use crate::{Error as _, Row, Value};
use futures::{future, Stream, TryStreamExt};
use indexmap::IndexMap;
use itertools::Itertools;
pub use postgres::types::Json;
use std::collections::HashMap;
use tokio_postgres as postgres;
use url::Url;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum Error {
	Postgres(#[from] postgres::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone, Debug)]
pub struct Options {
	pub url: Url,
	pub max_connections: usize,
}

#[derive(Default)]
pub struct Cache {
	statements: tokio::sync::Mutex<HashMap<String, postgres::Statement, fnv::FnvBuildHasher>>,
}

pub struct Database {
	options: Options,
	sender: async_channel::Sender<(postgres::Client, Cache)>,
	receiver: async_channel::Receiver<(postgres::Client, Cache)>,
}

pub struct Connection<'a> {
	marker: std::marker::PhantomData<&'a ()>,
	sender: async_channel::Sender<(postgres::Client, Cache)>,
	client: Option<postgres::Client>,
	cache: Option<Cache>,
}

pub struct Transaction<'a> {
	transaction: postgres::Transaction<'a>,
	cache: &'a Cache,
}

impl Cache {
	pub async fn get(
		&self,
		client: &impl postgres::GenericClient,
		query: impl AsRef<str>,
	) -> Result<postgres::Statement, Error> {
		if let Some(statement) = self.statements.lock().await.get(query.as_ref()) {
			return Ok(statement.clone());
		}
		let statement = client.prepare(query.as_ref()).await?;
		self.statements
			.lock()
			.await
			.insert(query.as_ref().to_owned(), statement.clone());
		Ok(statement)
	}
}

impl Database {
	pub async fn new(options: Options) -> Result<Self, Error> {
		let (sender, receiver) = async_channel::bounded(options.max_connections);
		for _ in 0..options.max_connections {
			let (client, connection) =
				postgres::connect(options.url.as_str(), postgres::NoTls).await?;
			tokio::spawn(async move {
				connection
					.await
					.inspect_err(|error| tracing::error!(?error, "postgres connection failed"))
					.ok();
			});
			let cache = Cache::default();
			sender.send((client, cache)).await.ok();
		}
		Ok(Self {
			options,
			sender,
			receiver,
		})
	}
}

impl super::Database for Database {
	type Error = Error;

	type Connection<'c> = Connection<'c> where Self: 'c;

	async fn connection(&self) -> Result<Self::Connection<'_>, Self::Error> {
		let sender = self.sender.clone();
		let (client, cache) = self
			.receiver
			.recv()
			.await
			.map_err(|_| Error::other("failed to acquire a database connection"))?;
		let mut client = Some(client);
		let cache = Some(cache);
		if client.as_ref().unwrap().is_closed() {
			let (client_, connection) =
				postgres::connect(self.options.url.as_str(), postgres::NoTls).await?;
			tokio::spawn(async move {
				connection
					.await
					.inspect_err(|error| tracing::error!(?error, "postgres connection failed"))
					.ok();
			});
			client.replace(client_);
		}
		Ok(Connection {
			marker: std::marker::PhantomData {},
			sender,
			client,
			cache,
		})
	}
}

impl<'c> super::Connection for Connection<'c> {
	type Error = Error;

	type Transaction<'t> = Transaction<'t> where Self: 't;

	async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
		let transaction = self.client.as_mut().unwrap().transaction().await?;
		let cache = self.cache.as_ref().unwrap();
		Ok(Transaction { transaction, cache })
	}
}

impl<'t> super::Transaction for Transaction<'t> {
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

impl<'a> super::Query for Connection<'a> {
	type Error = Error;

	async fn execute(&self, statement: String, params: Vec<Value>) -> Result<u64, Self::Error> {
		execute(
			self.client.as_ref().unwrap(),
			self.cache.as_ref().unwrap(),
			statement,
			params,
		)
		.await
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
		query(
			self.client.as_ref().unwrap(),
			self.cache.as_ref().unwrap(),
			statement,
			params,
		)
		.await
	}
}

impl<'a> super::Query for Transaction<'a> {
	type Error = Error;

	async fn execute(&self, statement: String, params: Vec<Value>) -> Result<u64, Self::Error> {
		execute(&self.transaction, self.cache, statement, params).await
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
		query(&self.transaction, self.cache, statement, params).await
	}
}

impl super::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl<'a> Drop for Connection<'a> {
	fn drop(&mut self) {
		let client = self.client.take().unwrap();
		let cache = self.cache.take().unwrap();
		self.sender.try_send((client, cache)).ok();
	}
}

async fn execute(
	client: &impl postgres::GenericClient,
	cache: &Cache,
	statement: String,
	params: Vec<Value>,
) -> Result<u64, Error> {
	let statement = cache.get(client, statement).await?;
	let params = &params
		.iter()
		.map(|value| value as &(dyn postgres::types::ToSql + Sync))
		.collect_vec();
	let n = client.execute(&statement, params).await?;
	Ok(n)
}

async fn query(
	client: &impl postgres::GenericClient,
	cache: &Cache,
	statement: String,
	params: Vec<Value>,
) -> Result<impl Stream<Item = Result<Row, Error>> + Send, Error> {
	let statement = cache.get(client, statement).await?;
	let rows = client.query_raw(&statement, params).await?;
	let rows = rows.map_err(Into::into).and_then(|row| {
		let mut entries = IndexMap::with_capacity(row.columns().len());
		for (i, column) in row.columns().iter().enumerate() {
			let name = column.name().to_owned();
			let value = row.get::<_, Value>(i);
			entries.insert(name, value);
		}
		let row = Row::with_entries(entries);
		future::ready(Ok(row))
	});
	Ok(rows)
}

impl postgres::types::ToSql for Value {
	fn to_sql(
		&self,
		ty: &postgres::types::Type,
		out: &mut bytes::BytesMut,
	) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
	where
		Self: Sized,
	{
		match self {
			Value::Null => Ok(postgres::types::IsNull::Yes),
			Value::Integer(value) => value.to_sql(ty, out),
			Value::Real(value) => value.to_sql(ty, out),
			Value::Text(value) => value.to_sql(ty, out),
			Value::Blob(value) => value.to_sql(ty, out),
		}
	}

	postgres::types::accepts!(BOOL, INT8, FLOAT8, TEXT, BYTEA);

	postgres::types::to_sql_checked!();
}

impl<'a> postgres::types::FromSql<'a> for Value {
	fn from_sql(
		ty: &postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		match *ty {
			postgres::types::Type::BOOL => Ok(Self::Integer(bool::from_sql(ty, raw)?.into())),
			postgres::types::Type::INT8 | postgres::types::Type::NUMERIC => {
				Ok(Self::Integer(i64::from_sql(ty, raw)?))
			},
			postgres::types::Type::FLOAT8 => Ok(Self::Real(f64::from_sql(ty, raw)?)),
			postgres::types::Type::TEXT => Ok(Self::Text(String::from_sql(ty, raw)?)),
			postgres::types::Type::BYTEA => Ok(Self::Blob(<Vec<u8>>::from_sql(ty, raw)?)),
			_ => Err("invalid type".into()),
		}
	}

	fn from_sql_null(
		_: &postgres::types::Type,
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(Self::Null)
	}

	postgres::types::accepts!(BOOL, INT8, NUMERIC, FLOAT8, TEXT, BYTEA);
}
