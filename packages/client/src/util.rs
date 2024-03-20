use bytes::Bytes;
use std::path::Path;
use tangram_error::{error, Result};

pub struct BytesBase64;

impl serde_with::SerializeAs<Bytes> for BytesBase64 {
	fn serialize_as<S>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let string = data_encoding::BASE64.encode(value);
		serializer.serialize_str(&string)
	}
}

impl<'de> serde_with::DeserializeAs<'de, Bytes> for BytesBase64 {
	fn deserialize_as<D>(deserializer: D) -> Result<Bytes, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor;

		impl<'de> serde::de::Visitor<'de> for Visitor {
			type Value = Bytes;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a base64 encoded string")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				let bytes = data_encoding::BASE64
					.decode(value.as_bytes())
					.map_err(|_| serde::de::Error::custom("invalid string"))?
					.into();
				Ok(bytes)
			}
		}

		deserializer.deserialize_any(Visitor)
	}
}

pub struct SeekFromString;

impl serde_with::SerializeAs<std::io::SeekFrom> for SeekFromString {
	fn serialize_as<S>(value: &std::io::SeekFrom, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let string = match value {
			std::io::SeekFrom::Start(seek) => format!("start.{seek}"),
			std::io::SeekFrom::End(seek) => format!("end.{seek}"),
			std::io::SeekFrom::Current(seek) => format!("current.{seek}"),
		};
		serializer.serialize_str(&string)
	}
}

impl<'de> serde_with::DeserializeAs<'de, std::io::SeekFrom> for SeekFromString {
	fn deserialize_as<D>(deserializer: D) -> Result<std::io::SeekFrom, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor;

		impl<'de> serde::de::Visitor<'de> for Visitor {
			type Value = std::io::SeekFrom;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a string")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				if let Some(seek) = value.strip_prefix("start.") {
					let seek = seek
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::Start(seek))
				} else if let Some(seek) = value.strip_prefix("end.") {
					let seek = seek
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::End(seek))
				} else if let Some(seek) = value.strip_prefix("current.") {
					let seek = seek
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::Current(seek))
				} else {
					let seek = value
						.parse()
						.map_err(|error| serde::de::Error::custom(error))?;
					Ok(std::io::SeekFrom::Start(seek))
				}
			}
		}

		deserializer.deserialize_str(Visitor)
	}
}

pub async fn rmrf(path: impl AsRef<Path>) -> Result<()> {
	let path = path.as_ref();

	// Get the metadata for the path.
	let metadata = match tokio::fs::metadata(path).await {
		Ok(metadata) => metadata,

		// If there is no file system object at the path, then return.
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			return Ok(());
		},

		Err(error) => {
			let path = path.display();
			return Err(error!(source = error, %path, "failed to get the metadata for the path"));
		},
	};

	if metadata.is_dir() {
		tokio::fs::remove_dir_all(path).await.map_err(|source| {
			let path = path.display();
			error!(!source, %path, "failed to remove the directory")
		})?;
	} else {
		tokio::fs::remove_file(path).await.map_err(|source| {
			let path = path.display();
			error!(!source, %path, "failed to remove the file")
		})?;
	};

	Ok(())
}
