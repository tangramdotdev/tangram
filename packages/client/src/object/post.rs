use crate as tg;
use bytes::Bytes;
use futures::{future, Stream, StreamExt as _, TryStreamExt as _};
use num::ToPrimitive;
use std::pin::Pin;
use tangram_futures::{read::Ext as _, write::Ext as _};
use tangram_http::{request::builder::Ext as _, response::Ext as _};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};

#[derive(Debug, Clone)]
pub struct Item {
	pub id: tg::object::Id,
	pub bytes: Bytes,
}

#[derive(Debug, Clone)]
pub enum Event {
	Complete(tg::object::Id),
	Incomplete(tg::object::Id),
	End,
}

impl tg::Client {
	pub(crate) async fn post_objects(
		&self,
		stream: Pin<
			Box<dyn Stream<Item = crate::Result<crate::object::post::Item>> + Send + 'static>,
		>,
	) -> tg::Result<impl Stream<Item = crate::Result<crate::object::post::Event>> + Send + 'static>
	{
		let method = http::Method::POST;
		let uri = "/objects";

		// Create the stream.
		let stream = stream.then(|result| async {
			let frame = match result {
				Ok(item) => {
					let bytes = item.serialize().await;
					hyper::body::Frame::data(bytes)
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = serde_json::to_string(&error).unwrap();
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(frame)
		});

		// Create the request.
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.stream(stream)
			.unwrap();

		// Send the request.
		let response = self.send(request).await?;

		// Check for an error.
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}

		// Create stream from the response.
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

		Ok(stream)
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;
	fn try_from(value: Event) -> Result<Self, Self::Error> {
		match value {
			Event::Complete(id) => Ok(tangram_http::sse::Event {
				event: Some("complete".to_owned()),
				data: id.to_string(),
				..tangram_http::sse::Event::default()
			}),
			Event::Incomplete(id) => Ok(tangram_http::sse::Event {
				event: Some("incomplete".to_owned()),
				data: id.to_string(),
				..tangram_http::sse::Event::default()
			}),
			Event::End => Ok(tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..tangram_http::sse::Event::default()
			}),
		}
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;
	fn try_from(value: tangram_http::sse::Event) -> Result<Self, Self::Error> {
		match value.event.as_deref() {
			Some("complete") => value
				.data
				.parse()
				.map_err(
					|source| tg::error!(!source, %data = value.data, "failed to deserialize the event"),
				)
				.map(Event::Complete),
			Some("incomplete") => value
				.data
				.parse()
				.map_err(
					|source| tg::error!(!source, %data = value.data, "failed to deserialize the event"),
				)
				.map(Event::Incomplete),
			Some("end") => Ok(Event::End),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|_| tg::error!("failed to deserialize the event"))?;
				Err(error)
			},
			Some(event) => Err(tg::error!(%event, "unknown event type")),
			None => Err(tg::error!("missing event type")),
		}
	}
}

impl Item {
	pub async fn serialize(&self) -> Bytes {
		// Serialize the ID.
		let id = self.id.to_string();

		// Allocate the body.
		let mut body = Vec::with_capacity(16 + id.len() + self.bytes.len());

		// Create the body.
		body.write_uvarint(id.len().to_u64().unwrap())
			.await
			.unwrap();
		body.write_all(id.as_bytes()).await.unwrap();
		body.write_uvarint(self.bytes.len().to_u64().unwrap())
			.await
			.unwrap();
		body.write_all(&self.bytes).await.unwrap();

		body.into()
	}

	pub async fn try_deserialize(
		mut reader: impl AsyncRead + Unpin + Send,
	) -> tg::Result<Option<Self>> {
		// Deserialize the ID.
		let Some(len) = reader
			.try_read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read id length"))?
			.map(|u| u.to_usize().unwrap())
		else {
			return Ok(None);
		};

		let mut id = vec![0u8; len];
		reader
			.read_exact(&mut id)
			.await
			.map_err(|source| tg::error!(!source, "failed to read id"))?;
		let id = String::from_utf8(id)
			.map_err(|_| tg::error!("expected a string"))?
			.parse::<tg::object::Id>()?;

		// Deserialize the data.
		let len = reader
			.read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read bytes length"))?
			.to_usize()
			.unwrap();
		let mut bytes = vec![0u8; len];
		reader
			.read_exact(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the bytes"))?;
		let bytes = Bytes::from(bytes);

		// Validate the data matches the ID.
		if id != tg::object::Id::new(id.kind(), &bytes) {
			return Err(tg::error!("id mismatch"));
		}

		// Create the object.
		Ok(Some(Item { id, bytes }))
	}
}
