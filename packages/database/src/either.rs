use crate::{Connection, Database, Query, Row, Transaction, Value};
use either::Either;
use futures::{
	Future, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
};

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error<L, R> {
	Either(Either<L, R>),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl<L, R> crate::Error for Error<L, R>
where
	L: crate::Error,
	R: crate::Error,
{
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl<L, R> Database for Either<L, R>
where
	L: Database,
	R: Database,
{
	type Error = Error<L::Error, R::Error>;

	type T = Either<L::T, R::T>;

	fn connection(&self) -> impl Future<Output = Result<Self::T, Self::Error>> {
		match self {
			Either::Left(left) => left
				.connection()
				.map_ok(Either::Left)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.connection()
				.map_ok(Either::Right)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}
}

impl<L, R> Connection for Either<L, R>
where
	L: Connection,
	R: Connection,
{
	type Error = Error<L::Error, R::Error>;

	type Transaction<'t> = Either<L::Transaction<'t>, R::Transaction<'t>> where Self: 't;

	fn transaction(&mut self) -> impl Future<Output = Result<Self::Transaction<'_>, Self::Error>> {
		match self {
			Either::Left(left) => left
				.transaction()
				.map_ok(Either::Left)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.transaction()
				.map_ok(Either::Right)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}
}

impl<L, R> Transaction for Either<L, R>
where
	L: Transaction,
	R: Transaction,
{
	type Error = Error<L::Error, R::Error>;

	fn rollback(self) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(left) => left
				.rollback()
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.rollback()
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn commit(self) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(left) => left
				.commit()
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.commit()
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}
}

impl<L, R> Query for Either<L, R>
where
	L: Query,
	R: Query,
{
	type Error = Error<L::Error, R::Error>;

	fn p(&self) -> &'static str {
		match self {
			Either::Left(left) => left.p(),
			Either::Right(right) => right.p(),
		}
	}

	fn execute(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<u64, Self::Error>> {
		match self {
			Either::Left(left) => left
				.execute(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.execute(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query(
		&self,
		statement: String,
		params: Vec<crate::Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Row, Self::Error>>, Self::Error>> {
		match self {
			Either::Left(left) => left
				.query(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Right(error)))
						.right_stream()
				})
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Value, Self::Error>> + Send, Self::Error>>
	{
		match self {
			Either::Left(left) => left
				.query_value(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_value(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Right(error)))
						.right_stream()
				})
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T, Self::Error>> + Send, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_into(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_into(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Right(error)))
						.right_stream()
				})
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T, Self::Error>>, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_value_into(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_value_into(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Right(error)))
						.right_stream()
				})
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_optional(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Row>, Self::Error>> {
		match self {
			Either::Left(left) => left
				.query_optional(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_optional(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_optional_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Value>, Self::Error>> {
		match self {
			Either::Left(left) => left
				.query_optional_value(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_optional_value(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_optional_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_optional_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_optional_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_optional_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_optional_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_optional_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Row, Self::Error>> {
		match self {
			Either::Left(left) => left
				.query_one(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_one(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Value, Self::Error>> {
		match self {
			Either::Left(left) => left
				.query_one_value(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_one_value(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_one_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_one_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_one_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_one_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Row>, Self::Error>> {
		match self {
			Either::Left(left) => left
				.query_all(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_all(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all_value(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Value>, Self::Error>> {
		match self {
			Either::Left(left) => left
				.query_all_value(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_all_value(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_all_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_all_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all_value_into<T>(
		&self,
		statement: String,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>, Self::Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		match self {
			Either::Left(left) => left
				.query_all_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(right) => right
				.query_all_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}
}
