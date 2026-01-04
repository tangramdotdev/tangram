use {super::Error, crate::Error as _, rusqlite as sqlite, std::marker::PhantomData};

pub use sqlite::types::Value;

pub trait DeserializeAs<T> {
	fn deserialize_as(value: sqlite::types::Value) -> Result<T, Error>;
}

impl<T, U> DeserializeAs<Option<T>> for Option<U>
where
	U: DeserializeAs<T>,
{
	fn deserialize_as(value: sqlite::types::Value) -> Result<Option<T>, Error> {
		if matches!(value, sqlite::types::Value::Null) {
			Ok(None)
		} else {
			U::deserialize_as(value).map(Some)
		}
	}
}

pub struct TryFrom<T>(PhantomData<T>);

impl<T, U, E> DeserializeAs<U> for TryFrom<T>
where
	T: sqlite::types::FromSql,
	U: std::convert::TryFrom<T, Error = E>,
	E: std::error::Error + Send + Sync + 'static,
{
	fn deserialize_as(value: sqlite::types::Value) -> Result<U, Error> {
		T::column_result((&value).into())
			.map_err(|error| Error::Other(error.into()))?
			.try_into()
			.map_err(Error::other)
	}
}

pub struct FromStr;

impl<T, E> DeserializeAs<T> for FromStr
where
	T: std::str::FromStr<Err = E>,
	E: std::error::Error + Send + Sync + 'static,
{
	fn deserialize_as(value: sqlite::types::Value) -> Result<T, Error> {
		let sqlite::types::Value::Text(s) = value else {
			return Err(Error::other("expected text for FromStr"));
		};
		s.parse().map_err(Error::other)
	}
}

impl<T> DeserializeAs<T> for crate::value::Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize_as(value: sqlite::types::Value) -> Result<T, Error> {
		let sqlite::types::Value::Text(json) = value else {
			return Err(Error::other("expected text for json"));
		};
		serde_json::from_str(&json).map_err(Error::other)
	}
}
