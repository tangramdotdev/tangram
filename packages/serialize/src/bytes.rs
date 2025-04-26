use crate::{Deserialize, Serialize};
use bytes::Bytes;

impl Serialize for Bytes {
	fn serialize<W>(&self, serializer: &mut crate::Serializer<W>) -> std::io::Result<()>
	where
		W: std::io::Write,
	{
		serializer.serialize_bytes(self)
	}
}

impl Deserialize for Bytes {
	fn deserialize<R>(deserializer: &mut crate::Deserializer<R>) -> std::io::Result<Self>
	where
		R: std::io::Read,
	{
		Ok(Bytes::from(deserializer.deserialize_bytes()?))
	}
}
