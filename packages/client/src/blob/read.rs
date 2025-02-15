use crate::{
	self as tg,
	handle::Ext as _,
	util::serde::{BytesBase64, SeekFromString},
	Client,
};
use bytes::Bytes;
use futures::{future, Stream, StreamExt as _, TryStreamExt as _};
use serde_with::serde_as;
use std::pin::pin;
use tangram_http::{request::builder::Ext as _, response::Ext as _};
use tokio::io::{AsyncBufRead, AsyncReadExt};
use tokio_util::io::StreamReader;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,
}

pub enum Event {
	Chunk(Chunk),
	End,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Chunk {
	pub position: u64,
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,
}

impl tg::Blob {
	pub async fn read<H>(
		&self,
		handle: &H,
		arg: tg::blob::read::Arg,
	) -> tg::Result<impl AsyncBufRead + Send + 'static>
	where
		H: tg::Handle,
	{
		let handle = handle.clone();
		let id = self.id(&handle).await?.clone();
		let stream = handle
			.try_read_blob(&id, arg)
			.await?
			.ok_or_else(|| tg::error!("expected a blob"))?
			.boxed();
		let reader = StreamReader::new(
			stream
				.map_ok(|chunk| chunk.bytes)
				.map_err(std::io::Error::other),
		);
		Ok(reader)
	}

	pub async fn bytes<H>(&self, handle: &H) -> tg::Result<Vec<u8>>
	where
		H: tg::Handle,
	{
		let mut bytes = Vec::new();
		let reader = self.read(handle, tg::blob::read::Arg::default()).await?;
		pin!(reader)
			.read_to_end(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read to the end"))?;
		Ok(bytes)
	}

	pub async fn text<H>(&self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let bytes = self.bytes(handle).await?;
		let string = String::from_utf8(bytes)
			.map_err(|source| tg::error!(!source, "failed to decode the blob's bytes as UTF-8"))?;
		Ok(string)
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Chunk(chunk) => {
				let data = serde_json::to_string(&chunk)
					.map_err(|source| tg::error!(!source, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					..Default::default()
				}
			},
			Event::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			None => {
				let chunk = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok(Self::Chunk(chunk))
			},
			Some("end") => Ok(Self::End),
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl Client {
	pub async fn try_read_blob_stream(
		&self,
		id: &tg::blob::Id,
		arg: Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<Event>> + Send + 'static>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/blobs/{id}/read?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
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
			Some((mime::TEXT, mime::EVENT_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = response
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			});
		Ok(Some(stream))
	}
}
