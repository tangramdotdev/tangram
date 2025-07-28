use crate::{Deserialize, Serialize};
use std::io::{Error, Read, Result, Write};
use url::Url;

impl Serialize for Url {
	fn serialize<W>(&self, serializer: &mut crate::Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize(self.as_str())
	}
}

impl Deserialize for Url {
	fn deserialize<R>(deserializer: &mut crate::Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let value = deserializer.deserialize_string()?;
		let url: Url = value.parse().map_err(Error::other)?;
		Ok(url)
	}
}
