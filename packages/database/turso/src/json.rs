#[derive(Debug, Default)]
pub struct Json<T>(pub T);

impl<T> TryFrom<turso::Value> for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	type Error = Box<dyn std::error::Error + Send + Sync>;

	fn try_from(value: turso::Value) -> Result<Self, Self::Error> {
		let turso::Value::Text(json) = value else {
			return Err("expected text for json".into());
		};
		let value = serde_json::from_str(&json)?;
		Ok(Self(value))
	}
}

impl<T> Json<T>
where
	T: serde::Serialize,
{
	pub fn into_turso_value(self) -> Result<turso::Value, serde_json::Error> {
		let json = serde_json::to_string(&self.0)?;
		Ok(turso::Value::Text(json))
	}
}
