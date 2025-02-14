use crate as tg;
use bytes::Bytes;
use futures::{stream, Stream};
use num::ToPrimitive as _;
use std::pin::Pin;
use tangram_either::Either;
use tangram_futures::{read::Ext as _, write::Ext as _};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Event {
	Process {
		id: tg::process::Id,
		metadata: tg::process::Metadata,
		data: tg::process::Data,
	},
	Object {
		id: tg::object::Id,
		metadata: tg::object::Metadata,
		bytes: Bytes,
	},
}

impl tg::Client {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		Ok(stream::empty())
	}
}

impl Event {
	pub async fn to_bytes(&self) -> Bytes {
		let mut bytes = Vec::new();
		self.to_writer(&mut bytes).await.unwrap();
		bytes.into()
	}

	pub async fn to_writer(&self, mut writer: impl AsyncWrite + Unpin + Send) -> tg::Result<()> {
		match self {
			Event::Process { id, metadata, data } => {
				let id = id.to_string();
				writer
					.write_uvarint(id.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id length"))?;
				writer
					.write_all(id.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id"))?;
				let metadata = serde_json::to_vec(metadata)
					.map_err(|source| tg::error!(!source, "failed to serialize the metadata"))?;
				writer
					.write_uvarint(metadata.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the metadata length"))?;
				writer.write_all(&metadata).await.unwrap();
				let data = serde_json::to_vec(data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				writer
					.write_uvarint(data.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the data length"))?;
				writer
					.write_all(&data)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the data"))?;
			},

			Event::Object {
				id,
				metadata,
				bytes,
			} => {
				let id = id.to_string();
				writer
					.write_uvarint(id.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id length"))?;
				writer
					.write_all(id.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the id"))?;
				let metadata = serde_json::to_vec(metadata)
					.map_err(|source| tg::error!(!source, "failed to serialize the metadata"))?;
				writer
					.write_uvarint(metadata.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the metadata length"))?;
				writer.write_all(&metadata).await.unwrap();
				writer
					.write_uvarint(bytes.len().to_u64().unwrap())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the bytes length"))?;
				writer
					.write_all(bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the bytes"))?;
			},
		}
		Ok(())
	}

	pub async fn from_reader(
		mut reader: impl AsyncRead + Unpin + Send,
	) -> tg::Result<Option<Self>> {
		// Read the ID.
		let Some(len) = reader
			.try_read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the id length"))?
			.map(|u| u.to_usize().unwrap())
		else {
			return Ok(None);
		};
		let mut id = vec![0u8; len];
		reader
			.read_exact(&mut id)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the id"))?;
		let id = String::from_utf8(id)
			.map_err(|_| tg::error!("expected a string"))?
			.parse()
			.map_err(Either::into_inner)?;

		let event = match id {
			Either::Left(id) => {
				// Read the metadata.
				let len = reader
					.read_uvarint()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the metadata length"))?
					.to_usize()
					.unwrap();
				let mut metadata = vec![0u8; len];
				reader
					.read_exact(&mut metadata)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the metadata"))?;
				let metadata = serde_json::from_slice(&metadata)
					.map_err(|source| tg::error!(!source, "failed to deserialize the metadata"))?;

				// Read the data.
				let len = reader
					.read_uvarint()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data length"))?
					.to_usize()
					.unwrap();
				let mut data = vec![0u8; len];
				reader
					.read_exact(&mut data)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data"))?;
				let data = serde_json::from_slice(&data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;

				Event::Process { id, metadata, data }
			},
			Either::Right(id) => {
				// Read the metadata.
				let len = reader
					.read_uvarint()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the metadata length"))?
					.to_usize()
					.unwrap();
				let mut metadata = vec![0u8; len];
				reader
					.read_exact(&mut metadata)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the metadata"))?;
				let metadata = serde_json::from_slice(&metadata)
					.map_err(|source| tg::error!(!source, "failed to deserialize the metadata"))?;

				// Read the data.
				let len = reader
					.read_uvarint()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data length"))?
					.to_usize()
					.unwrap();
				let mut bytes = vec![0u8; len];
				reader
					.read_exact(&mut bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the data"))?;
				let bytes = Bytes::from(bytes);

				Event::Object {
					id,
					metadata,
					bytes,
				}
			},
		};

		Ok(Some(event))
	}
}
