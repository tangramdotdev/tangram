use {
	super::Error,
	crate::Error as _,
	std::marker::PhantomData,
	tokio_postgres::{
		self as postgres, fallible_iterator::FallibleIterator as _, types::FromSql as _,
	},
};

pub trait DeserializeAs<T> {
	fn deserialize_as(ty: &postgres::types::Type, raw: Option<&[u8]>) -> Result<T, Error>;
}

impl<T, U> DeserializeAs<Option<T>> for Option<U>
where
	U: DeserializeAs<T>,
{
	fn deserialize_as(ty: &postgres::types::Type, raw: Option<&[u8]>) -> Result<Option<T>, Error> {
		match raw {
			None => Ok(None),
			Some(raw) => U::deserialize_as(ty, Some(raw)).map(Some),
		}
	}
}

impl<T, U> DeserializeAs<Vec<T>> for Vec<U>
where
	U: DeserializeAs<T>,
{
	fn deserialize_as(ty: &postgres::types::Type, raw: Option<&[u8]>) -> Result<Vec<T>, Error> {
		let raw = raw.ok_or_else(|| Error::other("expected array, got null"))?;
		let postgres::types::Kind::Array(element_type) = ty.kind() else {
			return Err(Error::other("expected array type"));
		};
		let array = postgres_protocol::types::array_from_sql(raw).map_err(Error::other)?;
		array
			.values()
			.map(|value| {
				U::deserialize_as(element_type, value).map_err(|error| {
					Box::new(error) as Box<dyn std::error::Error + Send + Sync + 'static>
				})
			})
			.collect()
			.map_err(Error::other)
	}
}

pub struct TryFrom<T>(PhantomData<T>);

impl<T, U, E> DeserializeAs<U> for TryFrom<T>
where
	T: postgres::types::FromSqlOwned,
	U: std::convert::TryFrom<T, Error = E>,
	E: std::error::Error + Send + Sync + 'static,
{
	fn deserialize_as(ty: &postgres::types::Type, raw: Option<&[u8]>) -> Result<U, Error> {
		T::from_sql_nullable(ty, raw)
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
	fn deserialize_as(ty: &postgres::types::Type, raw: Option<&[u8]>) -> Result<T, Error> {
		let s = String::from_sql_nullable(ty, raw).map_err(Error::other)?;
		s.parse().map_err(Error::other)
	}
}

impl<T> DeserializeAs<T> for crate::value::Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize_as(ty: &postgres::types::Type, raw: Option<&[u8]>) -> Result<T, Error> {
		let s = String::from_sql_nullable(ty, raw).map_err(Error::other)?;
		serde_json::from_str(&s).map_err(Error::other)
	}
}

pub struct Raw {
	ty: postgres::types::Type,
	raw: Option<Vec<u8>>,
}

impl Raw {
	#[must_use]
	pub fn ty(&self) -> &postgres::types::Type {
		&self.ty
	}

	#[must_use]
	pub fn raw(&self) -> Option<&[u8]> {
		self.raw.as_deref()
	}
}

impl<'a> postgres::types::FromSql<'a> for Raw {
	fn from_sql(
		ty: &postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
		Ok(Raw {
			ty: ty.clone(),
			raw: Some(raw.to_vec()),
		})
	}

	fn from_sql_null(
		ty: &postgres::types::Type,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
		Ok(Raw {
			ty: ty.clone(),
			raw: None,
		})
	}

	fn accepts(_ty: &postgres::types::Type) -> bool {
		true
	}
}
