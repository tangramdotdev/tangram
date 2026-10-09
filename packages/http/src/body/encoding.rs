use crate::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encoding {
	Json,
	Tangram,
}

impl Encoding {
	pub fn from_content_type(value: Option<&http::HeaderValue>) -> Result<Self> {
		let Some(value) = value else {
			return Ok(Self::Json);
		};
		let value: mime::Mime = value.to_str()?.parse()?;
		let encoding = if value.type_() == mime::APPLICATION
			&& value.subtype().as_str().starts_with("vnd.tangram.")
			&& value.suffix() != Some(mime::JSON)
		{
			Self::Tangram
		} else {
			Self::Json
		};
		Ok(encoding)
	}

	pub fn require_json(self) -> Result<()> {
		if self != Self::Json {
			return Err(std::io::Error::other(
				"the body prefix does not support Tangram serialization",
			)
			.into());
		}
		Ok(())
	}

	pub fn serialize<T>(self, value: &T) -> Result<Vec<u8>>
	where
		T: serde::Serialize + tangram_serialize::Serialize,
	{
		let bytes = match self {
			Self::Json => serde_json::to_vec(value)?,
			Self::Tangram => tangram_serialize::to_vec(value)?,
		};
		Ok(bytes)
	}

	pub fn deserialize<T>(self, bytes: &[u8]) -> Result<T>
	where
		T: serde::de::DeserializeOwned + for<'de> tangram_serialize::Deserialize<'de>,
	{
		let value = match self {
			Self::Json => serde_json::from_slice(bytes)?,
			Self::Tangram => tangram_serialize::from_slice(bytes)?,
		};
		Ok(value)
	}
}
