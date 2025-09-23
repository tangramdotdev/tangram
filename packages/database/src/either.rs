use {
	crate::{Connection, ConnectionOptions, Database, Query, Row, Transaction, Value},
	futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _},
	std::borrow::Cow,
	tangram_either::Either,
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
	fn is_retry(&self) -> bool {
		match self {
			Self::Either(Either::Left(e)) => e.is_retry(),
			Self::Either(Either::Right(e)) => e.is_retry(),
			Self::Other(_) => false,
		}
	}

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

	type Connection = Either<L::Connection, R::Connection>;

	fn connection_with_options(
		&self,
		options: ConnectionOptions,
	) -> impl Future<Output = Result<Self::Connection, Self::Error>> {
		match self {
			Either::Left(s) => s
				.connection_with_options(options)
				.map_ok(Either::Left)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.connection_with_options(options)
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

	type Transaction<'t>
		= Either<L::Transaction<'t>, R::Transaction<'t>>
	where
		Self: 't;

	fn transaction(&mut self) -> impl Future<Output = Result<Self::Transaction<'_>, Self::Error>> {
		match self {
			Either::Left(s) => s
				.transaction()
				.map_ok(Either::Left)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
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
			Either::Left(s) => s
				.rollback()
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.rollback()
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn commit(self) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s
				.commit()
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
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
			Either::Left(s) => s.p(),
			Either::Right(s) => s.p(),
		}
	}

	fn execute(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<u64, Self::Error>> {
		match self {
			Either::Left(s) => s
				.execute(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.execute(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query(
		&self,
		statement: Cow<'static, str>,
		params: Vec<crate::Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Row, Self::Error>>, Self::Error>> {
		match self {
			Either::Left(s) => s
				.query(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
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
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Value, Self::Error>> + Send, Self::Error>>
	{
		match self {
			Either::Left(s) => s
				.query_value(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
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
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T, Self::Error>> + Send, Self::Error>>
	where
		T: crate::row::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_into(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
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
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<T, Self::Error>>, Self::Error>>
	where
		T: crate::value::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_value_into(statement, params)
				.map_ok(|stream| {
					stream
						.map_err(|error| Error::Either(Either::Left(error)))
						.left_stream()
				})
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
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
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Row>, Self::Error>> {
		match self {
			Either::Left(s) => s
				.query_optional(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_optional(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_optional_value(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<Value>, Self::Error>> {
		match self {
			Either::Left(s) => s
				.query_optional_value(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_optional_value(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_optional_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>, Self::Error>>
	where
		T: crate::row::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_optional_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_optional_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_optional_value_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Option<T>, Self::Error>>
	where
		T: crate::value::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_optional_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_optional_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Row, Self::Error>> {
		match self {
			Either::Left(s) => s
				.query_one(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_one(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one_value(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Value, Self::Error>> {
		match self {
			Either::Left(s) => s
				.query_one_value(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_one_value(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T, Self::Error>>
	where
		T: crate::row::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_one_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_one_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_one_value_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<T, Self::Error>>
	where
		T: crate::value::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_one_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_one_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Row>, Self::Error>> {
		match self {
			Either::Left(s) => s
				.query_all(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_all(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all_value(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<Value>, Self::Error>> {
		match self {
			Either::Left(s) => s
				.query_all_value(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_all_value(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>, Self::Error>>
	where
		T: crate::row::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_all_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_all_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}

	fn query_all_value_into<T>(
		&self,
		statement: Cow<'static, str>,
		params: Vec<Value>,
	) -> impl Future<Output = Result<Vec<T>, Self::Error>>
	where
		T: crate::value::Deserialize,
	{
		match self {
			Either::Left(s) => s
				.query_all_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_future(),
			Either::Right(s) => s
				.query_all_value_into(statement, params)
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_future(),
		}
	}
}
