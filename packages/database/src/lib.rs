pub use self::{row::Row, value::Value};
use derive_more::From;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use std::{future::Future, pin::pin};
use tangram_error::{error, Result};

pub mod postgres;
pub mod row;
pub mod sqlite;
pub mod value;

pub mod prelude {
	pub use super::{Connection as _, Database as _, Query as _, Transaction as _};
}

pub trait Database {
	type Connection<'c>: Connection
	where
		Self: 'c;

	fn connection(&self) -> impl Future<Output = Result<Self::Connection<'_>>> + Send;
}

pub trait Connection {
	type Transaction<'t>: Transaction
	where
		Self: 't;

	fn transaction(&mut self) -> impl Future<Output = Result<Self::Transaction<'_>>> + Send;
}

pub trait Transaction {
	fn rollback(self) -> impl Future<Output = Result<()>> + Send;

	fn commit(self) -> impl Future<Output = Result<()>> + Send;
}

pub trait Query {
	fn execute(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<u64>> + Send;

	fn query(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Row>> + Send>> + Send;

	fn query_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Value>> + Send>> + Send {
		self.query(statement, params).map_ok(|rows| {
			rows.map(|result| {
				result.and_then(|row| {
					row.into_values()
						.next()
						.ok_or_else(|| error!("expected a value"))
				})
			})
		})
	}

	fn query_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T>> + Send>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query(statement, params)
			.map_ok(|rows| rows.map(|result| result.and_then(T::deserialize)))
	}

	fn query_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T>>>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_value(statement, params)
			.map_ok(|rows| rows.map(|result| result.and_then(T::deserialize)))
	}

	fn query_optional(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Row>>> + Send {
		async fn into_first<T>(rows: T) -> Result<Option<Row>>
		where
			T: Stream<Item = Result<Row>>,
		{
			pin!(rows).try_next().await
		}
		self.query(statement, params).and_then(into_first)
	}

	fn query_optional_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Value>>> + Send {
		self.query_optional(statement, params).map(|result| {
			result.and_then(|option| {
				option
					.map(|row| row.into_values().next())
					.ok_or_else(|| error!("expected a value"))
			})
		})
	}

	fn query_optional_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional(statement, params)
			.map(|result| result.and_then(|option| option.map(T::deserialize).transpose()))
	}

	fn query_optional_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional_value(statement, params)
			.map(|result| result.and_then(|option| option.map(T::deserialize).transpose()))
	}

	fn query_one(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Row>> + Send {
		self.query_optional(statement, params)
			.map(|result| result.and_then(|option| option.ok_or_else(|| error!("expected a row"))))
	}

	fn query_one_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Value>> + Send {
		self.query_one(statement, params).map(|result| {
			result.and_then(|row| {
				row.into_values()
					.next()
					.ok_or_else(|| error!("expected a value"))
			})
		})
	}

	fn query_one_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one(statement, params)
			.map(|result| result.and_then(T::deserialize))
	}

	fn query_one_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one_value(statement, params)
			.map(|result| result.and_then(T::deserialize))
	}

	fn query_all(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Row>>> + Send {
		self.query(statement, params)
			.and_then(TryStreamExt::try_collect)
	}

	fn query_all_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Value>>> + Send {
		self.query_all(statement, params).map(|result| {
			result.and_then(|rows| {
				rows.into_iter()
					.map(|row| {
						row.into_values()
							.next()
							.ok_or_else(|| error!("expected a value"))
					})
					.try_collect()
			})
		})
	}

	fn query_all_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all(statement, params).map(|result| {
			result.and_then(|rows| rows.into_iter().map(T::deserialize).try_collect())
		})
	}

	fn query_all_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all_value(statement, params).map(|result| {
			result.and_then(|rows| rows.into_iter().map(T::deserialize).try_collect())
		})
	}
}

#[macro_export]
macro_rules! params {
	($($v:expr),* $(,)?) => {
		vec![$(::serde::Serialize::serialize(&$v, $crate::value::ser::Serializer).unwrap(),)*]
	};
}
