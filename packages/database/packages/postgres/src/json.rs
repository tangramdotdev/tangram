use tokio_postgres as postgres;

#[derive(Debug, Default)]
pub struct Json<T>(pub T);

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
