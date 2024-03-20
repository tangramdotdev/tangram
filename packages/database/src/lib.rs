pub use self::{row::Row, value::Value};
use derive_more::From;
use futures::{future, Stream, TryStreamExt};
use itertools::Itertools;
use std::borrow::Cow;
use tangram_error::{error, Result};

pub mod postgres;
pub mod row;
pub mod sqlite;
pub mod value;

pub enum Database {
	Sqlite(sqlite::Database),
	Postgres(postgres::Database),
}

pub enum Options {
	Sqlite(sqlite::Options),
	Postgres(postgres::Options),
}

pub enum Connection {
	Sqlite(tangram_pool::Guard<sqlite::Connection>),
	Postgres(tangram_pool::Guard<postgres::Connection>),
}

pub enum Transaction<'a> {
	Sqlite(sqlite::Transaction<'a>),
	Postgres(postgres::Transaction<'a>),
}

impl Database {
	pub async fn new(options: Options) -> Result<Self> {
		match options {
			Options::Sqlite(options) => Ok(Self::Sqlite(sqlite::Database::new(options).await?)),
			Options::Postgres(options) => {
				Ok(Self::Postgres(postgres::Database::new(options).await?))
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

	pub async fn connection(&self) -> Result<Connection> {
		match self {
			Self::Sqlite(database) => Ok(Connection::Sqlite(database.connection().await?)),
			Self::Postgres(database) => Ok(Connection::Postgres(database.connection().await?)),
		}
	}
}

impl Connection {
	#[must_use]
	pub fn p(&self) -> &'static str {
		match self {
			Self::Sqlite(_) => "?",
			Self::Postgres(_) => "$",
		}
	}

	pub async fn execute(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<u64> {
		match self {
			Self::Sqlite(s) => s.execute(statement, params).await,
			Self::Postgres(s) => s.execute(statement, params).await,
		}
	}

	pub async fn query(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Row>>> {
		match self {
			Self::Sqlite(s) => s.query(statement, params).await.map(future::Either::Left),
			Self::Postgres(s) => s.query(statement, params).await.map(future::Either::Right),
		}
	}

	pub async fn query_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Value>>> {
		self.query(statement, params).await.map(|stream| {
			stream.and_then(|row| {
				future::ready(
					row.into_values()
						.next()
						.ok_or_else(|| error!("expected a value")),
				)
			})
		})
	}

	pub async fn query_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<T>>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query(statement, params)
			.await
			.map(|stream| stream.and_then(|row| future::ready(T::deserialize(row))))
	}

	pub async fn query_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<T>>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_scalar(statement, params)
			.await
			.map(|stream| stream.and_then(|value| future::ready(T::deserialize(value))))
	}

	pub async fn query_optional(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Row>> {
		match self {
			Self::Sqlite(s) => s.query_optional(statement, params).await,
			Self::Postgres(s) => s.query_optional(statement, params).await,
		}
	}

	pub async fn query_optional_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Value>> {
		self.query_optional(statement, params)
			.await?
			.map(|row| {
				row.into_values()
					.next()
					.ok_or_else(|| error!("expected a value"))
			})
			.transpose()
	}

	pub async fn query_optional_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional(statement, params)
			.await?
			.map(T::deserialize)
			.transpose()
	}

	pub async fn query_optional_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional_scalar(statement, params)
			.await?
			.map(T::deserialize)
			.transpose()
	}

	pub async fn query_one(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Row> {
		match self {
			Self::Sqlite(s) => s.query_one(statement, params).await,
			Self::Postgres(s) => s.query_one(statement, params).await,
		}
	}

	pub async fn query_one_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Value> {
		self.query_one(statement, params).await.and_then(|row| {
			row.into_values()
				.next()
				.ok_or_else(|| error!("expected a value"))
		})
	}

	pub async fn query_one_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one(statement, params)
			.await
			.and_then(T::deserialize)
	}

	pub async fn query_one_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one_scalar(statement, params)
			.await
			.and_then(T::deserialize)
	}

	pub async fn query_all(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Row>> {
		match self {
			Self::Sqlite(s) => s.query_all(statement, params).await,
			Self::Postgres(s) => s.query_all(statement, params).await,
		}
	}

	pub async fn query_all_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Value>> {
		self.query_all(statement, params).await.and_then(|rows| {
			rows.into_iter()
				.map(|row| {
					row.into_values()
						.next()
						.ok_or_else(|| error!("expected a value"))
				})
				.try_collect()
		})
	}

	pub async fn query_all_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all(statement, params)
			.await?
			.into_iter()
			.map(T::deserialize)
			.try_collect()
	}

	pub async fn query_all_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all_scalar(statement, params)
			.await?
			.into_iter()
			.map(T::deserialize)
			.try_collect()
	}

	pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
		match self {
			Self::Sqlite(s) => Ok(Transaction::Sqlite(s.transaction().await?)),
			Self::Postgres(s) => Ok(Transaction::Postgres(s.transaction().await?)),
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

	pub async fn execute(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<u64> {
		match self {
			Self::Sqlite(s) => s.execute(statement, params).await,
			Self::Postgres(s) => s.execute(statement, params).await,
		}
	}

	pub async fn query(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Row>>> {
		match self {
			Self::Sqlite(s) => s.query(statement, params).await.map(future::Either::Left),
			Self::Postgres(s) => s.query(statement, params).await.map(future::Either::Right),
		}
	}

	pub async fn query_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<Value>>> {
		self.query(statement, params).await.map(|stream| {
			stream.and_then(|row| {
				future::ready(
					row.into_values()
						.next()
						.ok_or_else(|| error!("expected a value")),
				)
			})
		})
	}

	pub async fn query_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<T>>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query(statement, params)
			.await
			.map(|stream| stream.and_then(|row| future::ready(T::deserialize(row))))
	}

	pub async fn query_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<impl Stream<Item = Result<T>>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_scalar(statement, params)
			.await
			.map(|stream| stream.and_then(|value| future::ready(T::deserialize(value))))
	}

	pub async fn query_optional(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Row>> {
		match self {
			Self::Sqlite(s) => s.query_optional(statement, params).await,
			Self::Postgres(s) => s.query_optional(statement, params).await,
		}
	}

	pub async fn query_optional_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<Value>> {
		self.query_optional(statement, params)
			.await?
			.map(|row| {
				row.into_values()
					.next()
					.ok_or_else(|| error!("expected a value"))
			})
			.transpose()
	}

	pub async fn query_optional_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional(statement, params)
			.await?
			.map(T::deserialize)
			.transpose()
	}

	pub async fn query_optional_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Option<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional_scalar(statement, params)
			.await?
			.map(T::deserialize)
			.transpose()
	}

	pub async fn query_one(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Row> {
		match self {
			Self::Sqlite(s) => s.query_one(statement, params).await,
			Self::Postgres(s) => s.query_one(statement, params).await,
		}
	}

	pub async fn query_one_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Value> {
		self.query_one(statement, params).await.and_then(|row| {
			row.into_values()
				.next()
				.ok_or_else(|| error!("expected a value"))
		})
	}

	pub async fn query_one_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one(statement, params)
			.await
			.and_then(T::deserialize)
	}

	pub async fn query_one_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one_scalar(statement, params)
			.await
			.and_then(T::deserialize)
	}

	pub async fn query_all(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Row>> {
		match self {
			Self::Sqlite(s) => s.query_all(statement, params).await,
			Self::Postgres(s) => s.query_all(statement, params).await,
		}
	}

	pub async fn query_all_scalar(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<Value>> {
		self.query_all(statement, params).await.and_then(|rows| {
			rows.into_iter()
				.map(|row| {
					row.into_values()
						.next()
						.ok_or_else(|| error!("expected a value"))
				})
				.try_collect()
		})
	}

	pub async fn query_all_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all(statement, params)
			.await?
			.into_iter()
			.map(T::deserialize)
			.try_collect()
	}

	pub async fn query_all_scalar_into<T>(
		&self,
		statement: impl Into<Cow<'static, str>>,
		params: impl IntoIterator<Item = Value>,
	) -> Result<Vec<T>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all_scalar(statement, params)
			.await?
			.into_iter()
			.map(T::deserialize)
			.try_collect()
	}

	pub async fn rollback(self) -> Result<()> {
		match self {
			Transaction::Sqlite(transaction) => transaction.rollback().await,
			Transaction::Postgres(transaction) => transaction.rollback().await,
		}
	}

	pub async fn commit(self) -> Result<()> {
		match self {
			Transaction::Sqlite(transaction) => transaction.commit().await,
			Transaction::Postgres(transaction) => transaction.commit().await,
		}
	}
}

#[macro_export]
macro_rules! params {
	($($v:expr),* $(,)?) => {
		[$(::serde::Serialize::serialize(&$v, $crate::value::Serializer).unwrap(),)*]
	};
}
