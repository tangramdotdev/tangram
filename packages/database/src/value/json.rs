use crate::{
	Value,
	value::{Deserialize, DeserializeAs, Serialize},
};
#[cfg(feature = "sqlite")]
use rusqlite as sqlite;
#[cfg(feature = "postgres")]
use tokio_postgres as postgres;

#[derive(Debug, Default)]
pub struct Json<T>(pub T);

impl<T> serde::Serialize for Json<T>
where
	T: serde::Serialize,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let json = serde_json::to_string(&self.0).map_err(serde::ser::Error::custom)?;
		serializer.serialize_str(&json)
	}
}

impl<'de, T> serde::Deserialize<'de> for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let json = <String as serde::Deserialize>::deserialize(deserializer)?;
		let value = serde_json::from_str(&json).map_err(serde::de::Error::custom)?;
		Ok(Self(value))
	}
}

impl<T> Serialize for Json<T>
where
	T: serde::Serialize,
{
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let json = serde_json::to_string(&self.0)?;
		Ok(Value::Text(json))
	}
}

impl<T> Deserialize for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let json = value.try_unwrap_text()?;
		let value = serde_json::from_str(&json)?;
		Ok(Self(value))
	}
}

impl<T> DeserializeAs<T> for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize_as(
		value: Value,
	) -> Result<T, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let json = value.try_unwrap_text()?;
		let value = serde_json::from_str(&json)?;
		Ok(value)
	}
}

#[cfg(feature = "sqlite")]
impl<T> sqlite::types::ToSql for Json<T>
where
	T: serde::Serialize,
{
	fn to_sql(&self) -> sqlite::Result<sqlite::types::ToSqlOutput<'_>> {
		let json = serde_json::to_string(&self.0)
			.map_err(|error| sqlite::Error::ToSqlConversionFailure(error.into()))?;
		Ok(sqlite::types::ToSqlOutput::Owned(
			sqlite::types::Value::Text(json),
		))
	}
}

#[cfg(feature = "sqlite")]
impl<T> sqlite::types::FromSql for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn column_result(value: sqlite::types::ValueRef) -> sqlite::types::FromSqlResult<Self> {
		let json = value.as_str()?;
		let value = serde_json::from_str(json)
			.map_err(|error| sqlite::types::FromSqlError::Other(error.into()))?;
		Ok(Self(value))
	}
}

#[cfg(feature = "postgres")]
impl<T> postgres::types::ToSql for Json<T>
where
	T: serde::Serialize + std::fmt::Debug,
{
	fn to_sql(
		&self,
		ty: &postgres::types::Type,
		out: &mut bytes::BytesMut,
	) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Send + Sync>> {
		let json = serde_json::to_string(&self.0)?;
		postgres::types::ToSql::to_sql(&json, ty, out)
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		matches!(
			*ty,
			postgres::types::Type::TEXT
				| postgres::types::Type::JSON
				| postgres::types::Type::JSONB
		)
	}

	postgres::types::to_sql_checked!();
}

#[cfg(feature = "postgres")]
impl<'a, T> postgres::types::FromSql<'a> for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn from_sql(
		ty: &postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
		let json = <&str as postgres::types::FromSql>::from_sql(ty, raw)?;
		let value = serde_json::from_str(json)?;
		Ok(Self(value))
	}

	fn accepts(ty: &postgres::types::Type) -> bool {
		matches!(
			*ty,
			postgres::types::Type::TEXT
				| postgres::types::Type::JSON
				| postgres::types::Type::JSONB
		)
	}
}
