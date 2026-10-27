use tangram_client::prelude::*;

#[derive(Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Connected {
	#[tangram_serialize(id = 0)]
	pub lease: tg::process::Lease,
}

#[must_use]
pub fn connected_subject(id: &tg::process::Id) -> String {
	format!("processes.{id}.control.connected")
}

impl tangram_messenger::Payload for Connected {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}
}
