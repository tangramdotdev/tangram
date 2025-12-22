use {
	futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _},
	itertools::Itertools as _,
	std::{
		borrow::Cow,
		hash::{Hash, Hasher},
		pin::pin,
	},
};

pub use self::{pool::Priority, row::Row, value::Value};

pub mod either;
pub mod pool;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod row;
#[cfg(feature = "sqlite")]
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

	type Connection;

	fn connection(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
		self.connection_with_options(ConnectionOptions::default())
	}

	fn write_connection(
		&self,
	) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
		let options = ConnectionOptions {
			kind: ConnectionKind::Write,
			..Default::default()
		};
		self.connection_with_options(options)
	}

	fn connection_with_options(
		&self,
		options: ConnectionOptions,
	) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

	fn sync(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Clone, Debug, Default)]
pub struct ConnectionOptions {
	pub kind: ConnectionKind,
	pub priority: Priority,
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
		T: crate::row::Deserialize,
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
		T: crate::value::Deserialize,
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
		T: crate::row::Deserialize,
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
		T: crate::value::Deserialize,
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
		T: crate::row::Deserialize,
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
		T: crate::value::Deserialize,
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
		T: crate::row::Deserialize,
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
		T: crate::value::Deserialize,
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

#[derive(Clone)]
pub struct CacheKey(Cow<'static, str>);

impl CacheKey {
	#[must_use]
	pub fn new(s: Cow<'static, str>) -> Self {
		Self(s)
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		&self.0
	}
}

impl Hash for CacheKey {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match &self.0 {
			Cow::Borrowed(s) => {
				0u8.hash(state);
				(s.as_ptr() as usize).hash(state);
			},
			Cow::Owned(s) => {
				1u8.hash(state);
				s.hash(state);
			},
		}
	}
}

impl PartialEq for CacheKey {
	fn eq(&self, other: &Self) -> bool {
		match (&self.0, &other.0) {
			(Cow::Borrowed(a), Cow::Borrowed(b)) => std::ptr::eq(*a, *b),
			(Cow::Owned(a), Cow::Owned(b)) => a == b,
			_ => false,
		}
	}
}

impl Eq for CacheKey {}

#[macro_export]
macro_rules! params {
	($($v:expr),* $(,)?) => {
		vec![$($crate::value::Serialize::serialize(&$v).unwrap(),)*]
	};
}
