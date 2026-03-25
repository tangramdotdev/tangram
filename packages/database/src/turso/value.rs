use {super::Error, crate::Error as _, std::marker::PhantomData};

pub trait DeserializeAs<T> {
	fn deserialize_as(value: turso::Value) -> Result<T, Error>;
}

impl<T, U> DeserializeAs<Option<T>> for Option<U>
where
	U: DeserializeAs<T>,
{
	fn deserialize_as(value: turso::Value) -> Result<Option<T>, Error> {
		if matches!(value, turso::Value::Null) {
			Ok(None)
		} else {
			U::deserialize_as(value).map(Some)
		}
	}
}

pub struct TryFrom<T>(PhantomData<T>);

impl<T, U, E> DeserializeAs<U> for TryFrom<T>
where
	T: crate::value::Deserialize,
	U: std::convert::TryFrom<T, Error = E>,
	E: std::error::Error + Send + Sync + 'static,
{
	fn deserialize_as(value: turso::Value) -> Result<U, Error> {
		let value = super::from_turso_value(value);
		T::deserialize(value)
			.map_err(Error::other)?
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
	fn deserialize_as(value: turso::Value) -> Result<T, Error> {
		let turso::Value::Text(s) = value else {
			return Err(Error::other("expected text for FromStr"));
		};
		s.parse().map_err(Error::other)
	}
}

impl<T> DeserializeAs<T> for crate::value::Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize_as(value: turso::Value) -> Result<T, Error> {
		let turso::Value::Text(json) = value else {
			return Err(Error::other("expected text for json"));
		};
		serde_json::from_str(&json).map_err(Error::other)
	}
}
