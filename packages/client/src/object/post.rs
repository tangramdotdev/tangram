use crate as tg;
use bytes::Bytes;
use futures::{future, Stream, StreamExt as _, TryStreamExt as _};
use http_body_util::StreamBody;
use num::ToPrimitive;
use std::pin::Pin;
use tangram_http::{incoming::response::Ext as _, Outgoing};

#[derive(Debug, Clone)]
pub struct Item {
	pub id: tg::object::Id,
	pub bytes: Bytes,
}

#[derive(Debug, Clone)]
pub enum Event {
	Complete(tg::object::Id),
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

		// Create the body.
		let body = Outgoing::body(StreamBody::new(stream.map(|result| match result {
			Ok(item) => {
				let bytes = item.serialize();
				let frame = hyper::body::Frame::data(bytes);
				Ok(frame)
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = serde_json::to_string(&error).unwrap();
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				let frame = hyper::body::Frame::trailers(trailers);
				Ok(frame)
			},
		})));

		// Create the request.
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
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
				.map_err(|source| tg::error!(!source, "failed to deserialize the event"))
				.map(Event::Complete),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|_| tg::error!("failed to deserialize the event"))?;
				Err(error)
			},
			_ => Err(tg::error!("unknown event type")),
		}
	}
}

impl Item {
	pub fn serialize(&self) -> Bytes {
		// Serialize the ID.
		let id = self.id.to_string();

		// Allocate the body.
		let mut body = Vec::with_capacity(16 + id.len() + self.bytes.len());

		// Create the body.
		body.extend_from_slice(&id.len().to_u64().unwrap().to_be_bytes());
		body.extend_from_slice(id.as_bytes());
		body.extend_from_slice(&self.bytes.len().to_u64().unwrap().to_be_bytes());
		body.extend_from_slice(&self.bytes);

		body.into()
	}

	pub fn deserialize(mut bytes: Bytes) -> tg::Result<Self> {
		// Deserialize the ID length.
		let len = u64::from_be_bytes(
			bytes
				.split_to(8)
				.as_ref()
				.try_into()
				.map_err(|_| tg::error!("expected 8 bytes"))?,
		)
		.try_into()
		.unwrap();

		// Deserialize the id.
		let id = std::str::from_utf8(&bytes.split_to(len))
			.map_err(|_| tg::error!("expected a string"))?
			.parse::<tg::object::Id>()?;

		// Deserialize the data length.
		let len = u64::from_be_bytes(
			bytes
				.split_to(8)
				.as_ref()
				.try_into()
				.map_err(|_| tg::error!("expected 8 bytes"))?,
		)
		.try_into()
		.unwrap();

		// Get the data.
		let data = bytes.split_to(len);

		// Validate the arg is empty.
		if !bytes.is_empty() {
			return Err(tg::error!("size mismatch"));
		}

		// Validate the data matches the ID.
		if id != tg::object::Id::new(id.kind(), &data) {
			return Err(tg::error!("id mismatch"));
		}

		// Create the object.
		Ok(Item { id, bytes: data })
	}
}
