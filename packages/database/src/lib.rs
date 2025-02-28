use futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _};
use itertools::Itertools as _;
use std::{borrow::Cow, pin::pin};

pub use self::{pool::Priority, row::Row, value::Value};

pub mod either;
pub mod pool;
pub mod postgres;
pub mod row;
pub mod sqlite;
pub mod value;

pub mod prelude {
	pub use super::{Connection as _, Database as _, Error as _, Query as _, Transaction as _};
}

pub trait Error: std::error::Error + Send + Sync + 'static {
	fn is_retry(&self) -> bool;
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self;
}

pub trait Database {
	type Error: Error;

	type T;

	fn connection(&self) -> impl Future<Output = Result<Self::T, Self::Error>> + Send {
		self.connection_with_options(ConnectionOptions::default())
	}

	fn write_connection(&self) -> impl Future<Output = Result<Self::T, Self::Error>> + Send {
		let options = ConnectionOptions {
			kind: ConnectionKind::Write,
			..Default::default()
		};
		self.connection_with_options(options)
	}

	fn connection_with_options(
		&self,
		options: ConnectionOptions,
	) -> impl Future<Output = Result<Self::T, Self::Error>> + Send;
}

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
	pub kind: ConnectionKind,
	pub priority: Priority,
	pub timeout_threshold: std::time::Duration,
	pub timeout_warning_name: Option<String>,
}

impl Default for ConnectionOptions {
	fn default() -> Self {
		Self {
			kind: ConnectionKind::default(),
			priority: Priority::default(),
			timeout_threshold: std::time::Duration::from_secs(2),
			timeout_warning_name: None,
		}
	}
}

#[derive(Clone, Debug, Default)]
pub enum ConnectionKind {
	#[default]
	Read,
	Write,
}

pub trait Connection {
	type Error: Error;

	type Transaction<'t>: Transaction
	where
		Self: 't;

	fn transaction(
		&mut self,
	) -> impl Future<Output = Result<Self::Transaction<'_>, Self::Error>> + Send;
}

pub trait Transaction {
	type Error: Error;

	fn rollback(self) -> impl Future<Output = Result<(), Self::Error>> + Send;

	fn commit(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Query {
	type Error: Error;

	fn p(&self) -> &'static str;

	fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<u64, Self::Error>> + Send;

	fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error>>
	+ Send;

	fn query_value(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Value, Self::Error>> + Send, Self::Error>,
	> + Send {
		self.query(statement, params).map_ok(|rows| {
			rows.map(|result| {
				result.and_then(|row| {
					row.into_values()
						.next()
						.ok_or_else(|| Self::Error::other("expected a value"))
				})
			})
		})
	}

	fn query_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T, Self::Error>> + Send, Self::Error>>
	+ Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query(statement, params).map_ok(|rows| {
			rows.map(|result| {
				result.and_then(|row| T::deserialize(row).map_err(Self::Error::other))
			})
		})
	}

	fn query_value_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T, Self::Error>>, Self::Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_value(statement, params).map_ok(|rows| {
			rows.map(|result| {
				result.and_then(|value| T::deserialize(value).map_err(Self::Error::other))
			})
		})
	}

	fn query_optional(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Row>, Self::Error>> + Send {
		self.query(statement, params)
			.and_then(|rows| async { pin!(rows).try_next().await })
	}

	fn query_optional_value(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Value>, Self::Error>> + Send {
		self.query_optional(statement, params).map(|result| {
			result.and_then(|option| {
				option
					.map(|row| {
						row.into_values()
							.next()
							.ok_or_else(|| Self::Error::other("expected a value"))
					})
					.transpose()
			})
		})
	}

	fn query_optional_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>, Self::Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional(statement, params).map(|result| {
			result.and_then(|option| {
				option
					.map(|row| T::deserialize(row).map_err(Self::Error::other))
					.transpose()
			})
		})
	}

	fn query_optional_value_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>, Self::Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_optional_value(statement, params).map(|result| {
			result.and_then(|option| {
				option
					.map(|value| T::deserialize(value).map_err(Self::Error::other))
					.transpose()
			})
		})
	}

	fn query_one(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Row, Self::Error>> + Send {
		self.query_optional(statement, params).map(|result| {
			result.and_then(|option| option.ok_or_else(|| Self::Error::other("expected a row")))
		})
	}

	fn query_one_value(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Value, Self::Error>> + Send {
		self.query_one(statement, params).map(|result| {
			result.and_then(|row| {
				row.into_values()
					.next()
					.ok_or_else(|| Self::Error::other("expected a value"))
			})
		})
	}

	fn query_one_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T, Self::Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one(statement, params)
			.map(|result| result.and_then(|row| T::deserialize(row).map_err(Self::Error::other)))
	}

	fn query_one_value_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T, Self::Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_one_value(statement, params).map(|result| {
			result.and_then(|value| T::deserialize(value).map_err(Self::Error::other))
		})
	}

	fn query_all(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Row>, Self::Error>> + Send {
		self.query(statement, params)
			.and_then(futures::TryStreamExt::try_collect)
	}

	fn query_all_value(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Value>, Self::Error>> + Send {
		self.query_all(statement, params).map(|result| {
			result.and_then(|rows| {
				rows.into_iter()
					.map(|row| {
						row.into_values()
							.next()
							.ok_or_else(|| Self::Error::other("expected a value"))
					})
					.try_collect()
			})
		})
	}

	fn query_all_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>, Self::Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all(statement, params).map(|result| {
			result.and_then(|rows| {
				rows.into_iter()
					.map(|row| T::deserialize(row).map_err(Error::other))
					.try_collect()
			})
		})
	}

	fn query_all_value_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>, Self::Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.query_all_value(statement, params).map(|result| {
			result.and_then(|rows| {
				rows.into_iter()
					.map(|row| T::deserialize(row).map_err(Error::other))
					.try_collect()
			})
		})
	}
}

#[macro_export]
macro_rules! params {
	($($v:expr),* $(,)?) => {
		vec![$(::serde::Serialize::serialize(&$v, $crate::value::ser::Serializer).unwrap(),)*]
	};
}
pub(crate) async fn with_timeout_logging<F, T>(
	future: F,
	threshold: std::time::Duration,
	name: &str,
) -> T
where
	F: Future<Output = T>,
{
	let start = std::time::Instant::now();

	let timeout_future = tokio::spawn({
		let name = name.to_owned();
		async move {
			tokio::time::sleep(threshold).await;
			tracing::warn!("Operation '{name}' is taking longer than threshold: {threshold:?}");
		}
	});

	let result = future.await;
	timeout_future.abort();
	let elapsed = start.elapsed();
	if elapsed > threshold {
		tracing::debug!(?elapsed, "Operation '{name}' completed");
	}
	result
}
