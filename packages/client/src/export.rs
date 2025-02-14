use crate::{self as tg, util::serde::CommaSeparatedString};
use bytes::Bytes;
use futures::{stream, Stream, StreamExt as _};
use num::ToPrimitive as _;
use serde_with::serde_as;
use std::pin::Pin;
use tangram_either::Either;
use tangram_futures::{read::Ext as _, write::Ext as _};
use tangram_http::{request::builder::Ext as _, response::Ext as _};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueryArg {
	#[serde_as(as = "CommaSeparatedString")]
	items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Event {
	Complete(Either<tg::process::Id, tg::object::Id>),
}

#[derive(Debug, Clone)]
pub enum Item {
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
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Item>> + Send + 'static> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(QueryArg::from(arg)).unwrap();
		let uri = format!("/export?{query}");

		let sse = stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});

		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::ACCEPT,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.sse(sse)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}

		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if !matches!(
			content_type
				.as_ref()
				.map(|content_type| (content_type.type_(), content_type.subtype())),
			Some((mime::APPLICATION, mime::OCTET_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let reader = response.reader();
		let stream = stream::try_unfold(reader, |mut reader| async move {
			let Some(item) = tg::export::Item::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		});

		Ok(stream)
	}
}

impl Item {
	pub async fn to_bytes(&self) -> Bytes {
		let mut bytes = Vec::new();
		self.to_writer(&mut bytes).await.unwrap();
		bytes.into()
	}

	pub async fn to_writer(&self, mut writer: impl AsyncWrite + Unpin + Send) -> tg::Result<()> {
		match self {
			Item::Process { id, metadata, data } => {
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

			Item::Object {
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

		let item = match id {
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

				Item::Process { id, metadata, data }
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

				Item::Object {
					id,
					metadata,
					bytes,
				}
			},
		};

		Ok(Some(item))
	}
}

impl From<Arg> for QueryArg {
	fn from(value: Arg) -> Self {
		Self {
			items: value.items,
			remote: value.remote,
		}
	}
}

impl From<QueryArg> for Arg {
	fn from(value: QueryArg) -> Self {
		Self {
			items: value.items,
			remote: value.remote,
		}
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Complete(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("complete".to_owned()),
					data,
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("complete") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Complete(data))
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
