use {crate::Error, bytes::Bytes};

pub trait Payload: std::any::Any + Send + Sync + 'static {
	fn serialize(&self) -> Result<Bytes, Error>;

	fn deserialize(bytes: Bytes) -> Result<Self, Error>
	where
		Self: Sized;
}

impl Payload for Bytes {
	fn serialize(&self) -> Result<Bytes, Error> {
		Ok(self.clone())
	}

	fn deserialize(bytes: Bytes) -> Result<Self, Error> {
		Ok(bytes)
	}
}

impl Payload for () {
	fn serialize(&self) -> Result<Bytes, Error> {
		Ok(Bytes::new())
	}

	fn deserialize(_bytes: Bytes) -> Result<Self, Error> {
		Ok(())
	}
}

#[derive(Clone, Debug)]
pub struct Json<T>(pub T);

impl<T> Payload for Json<T>
where
	T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
	fn serialize(&self) -> Result<Bytes, Error> {
		serde_json::to_vec(&self.0)
			.map(Bytes::from)
			.map_err(Error::serialization)
	}

	fn deserialize(bytes: Bytes) -> Result<Self, Error> {
		serde_json::from_slice(&bytes)
			.map(Json)
			.map_err(Error::deserialization)
	}
}
