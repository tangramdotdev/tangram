use crate::{
	self as tg,
	util::serde::{CommaSeparatedString, is_false},
};
use futures::{Stream, StreamExt as _, TryStreamExt as _, future};
use serde_with::serde_as;
use std::pin::Pin;
use tangram_either::Either;
use tangram_http::{Body, response::Ext as _};

pub const CONTENT_TYPE: &str = "application/vnd.tangram.import";

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub items: Option<Vec<Either<tg::process::Id, tg::object::Id>>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap)]
pub enum Event {
	Complete(tg::import::Complete),
	Progress(tg::import::Progress),
	End,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Complete {
	Process(ProcessComplete),
	Object(ObjectComplete),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ProcessComplete {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands_complete: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	pub complete: bool,

	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs_complete: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ObjectComplete {
	pub id: tg::object::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Progress {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub processes: Option<u64>,
	pub objects: u64,
	pub bytes: u64,
}

impl tg::Client {
	pub async fn import(
		&self,
		arg: tg::import::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + use<>> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(arg).unwrap();
		let uri = format!("/import?{query}");

		// Create the body.
		let body = Body::with_stream(stream.then(|result| async {
			let frame = match result {
				Ok(event) => {
					let bytes = event.to_bytes().await;
					hyper::body::Frame::data(bytes)
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = serde_json::to_string(&error.to_data()).unwrap();
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(frame)
		}));

		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(http::header::CONTENT_TYPE, CONTENT_TYPE.to_string())
			.body(body)
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

		Ok(stream)
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Complete(data) => data.try_into()?,
			Event::Progress(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("progress".to_owned()),
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
			Some("complete") => {
				let data = Complete::try_from(value)?;
				Ok(Self::Complete(data))
			},
			Some("progress") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Progress(data))
			},
			Some("end") => Ok(Self::End),
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl TryFrom<Complete> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Complete) -> Result<Self, Self::Error> {
		let data = serde_json::to_string(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let event = tangram_http::sse::Event {
			event: Some("complete".to_owned()),
			data,
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Complete {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("complete") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(data)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
