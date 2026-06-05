use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::UuidBase32,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum RequestEvent {
	Request(Request),
	Stop,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ResponseEvent {
	Response(Response),
	Stop,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Response {
	#[serde_as(as = "UuidBase32")]
	pub id: uuid::Uuid,
	pub kind: ResponseKind,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Request {
	#[serde_as(as = "UuidBase32")]
	pub id: uuid::Uuid,
	pub kind: RequestKind,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum RequestKind {
	Read(ReadRequest),
	Write(WriteRequest),
	Signal(SignalRequest),
	Tty(TtyRequest),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ResponseKind {
	Read(ReadResponse),
	Write,
	Signal,
	Tty,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ReadRequest {
	pub stream: tg::process::stdio::Stream,
	pub len: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct WriteRequest {
	pub stream: tg::process::stdio::Stream,
	pub bytes: Bytes,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SignalRequest {
	pub signal: tg::process::signal::Signal,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TtyRequest {
	pub size: tg::process::tty::Size,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ReadResponse {
	pub stream: tg::process::stdio::Stream,
	pub bytes: Bytes,
}

impl tg::Session {
	pub async fn try_get_process_control_stream(
		&self,
		id: &tg::process::Id,
		stream: BoxStream<'static, tg::Result<tg::process::control::ResponseEvent>>,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::process::control::RequestEvent>>
			+ Send
			+ 'static
			+ use<>,
		>,
	> {
		let method = http::Method::POST;
		let path = format!("/processes/{id}/control");
		let uri = Uri::builder().path(&path).build().unwrap();
		let stream = stream.map(
			|result: tg::Result<tg::process::control::ResponseEvent>| match result {
				Ok(event) => event.try_into(),
				Err(error) => error.try_into(),
			},
		);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.sse(stream)
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
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
			.map_err(|error| tg::error!(!error, "failed to read an event"))
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

impl TryFrom<RequestEvent> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: RequestEvent) -> Result<Self, Self::Error> {
		let event = match value {
			RequestEvent::Request(request) => {
				let data = serde_json::to_string(&request)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("request".to_owned()),
					..Default::default()
				}
			},
			RequestEvent::Stop => tangram_http::sse::Event {
				event: Some("stop".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for RequestEvent {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("request") => {
				let request = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Request(request))
			},
			Some("stop") => Ok(Self::Stop),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl TryFrom<ResponseEvent> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: ResponseEvent) -> Result<Self, Self::Error> {
		let event = match value {
			ResponseEvent::Response(response) => {
				let data = serde_json::to_string(&response)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("response".to_owned()),
					..Default::default()
				}
			},
			ResponseEvent::Stop => tangram_http::sse::Event {
				event: Some("stop".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ResponseEvent {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("response") => {
				let response = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Response(response))
			},
			Some("stop") => Ok(Self::Stop),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
