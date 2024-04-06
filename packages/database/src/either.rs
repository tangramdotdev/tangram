use crate::{Query, Row, Value};
use either::Either;
use futures::{Stream, StreamExt as _, TryStreamExt as _};

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error<L, R> {
	Either(Either<L, R>),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl<L, R> super::Error for Error<L, R>
where
	L: super::Error,
	R: super::Error,
{
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl<L, R> Query for Either<L, R>
where
	L: Query + Sync,
	R: Query + Sync,
{
	type Error = Error<L::Error, R::Error>;

	async fn execute(&self, statement: String, params: Vec<Value>) -> Result<u64, Self::Error> {
		match self {
			Either::Left(left) => left
				.execute(statement, params)
				.await
				.map_err(|error| Error::Either(Either::Left(error))),
			Either::Right(right) => right
				.execute(statement, params)
				.await
				.map_err(|error| Error::Either(Either::Right(error))),
		}
	}

	async fn query(
		&self,
		statement: String,
		params: Vec<crate::Value>,
	) -> Result<impl Stream<Item = Result<Row, Self::Error>> + Send, Self::Error> {
		let stream = match self {
			Either::Left(left) => left
				.query(statement, params)
				.await
				.map_err(|error| Error::Either(Either::Left(error)))?
				.map_err(|error| Error::Either(Either::Left(error)))
				.left_stream(),
			Either::Right(right) => right
				.query(statement, params)
				.await
				.map_err(|error| Error::Either(Either::Right(error)))?
				.map_err(|error| Error::Either(Either::Right(error)))
				.right_stream(),
		};
		Ok(stream)
	}
}
