use {
	super::Stream,
	crate::prelude::*,
	futures::{TryStreamExt as _, stream::BoxStream},
	std::collections::BTreeMap,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

mod all;

pub(crate) use all::all;

pub mod stream;

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Ack {
	#[tangram_serialize(id = 0)]
	pub id: u64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub data: Data,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 2,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientMessage {
	#[tangram_serialize(id = 0)]
	Ack(Ack),
	#[tangram_serialize(id = 1)]
	Request(Request),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Data {
	#[tangram_serialize(id = 0)]
	Chunk(super::Chunk),
	#[tangram_serialize(id = 1)]
	End(End),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct End {
	#[tangram_serialize(id = 0)]
	pub combined_position: u64,
	#[tangram_serialize(id = 1)]
	pub stream_positions: BTreeMap<Stream, u64>,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
	pub streams: Vec<Stream>,
}

/// A chunk completes in full unless the stream closes; length includes bytes committed by an earlier attempt.
#[derive(
	Clone,
	Copy,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Output {
	#[tangram_serialize(id = 0)]
	pub closed: bool,
	#[tangram_serialize(id = 1)]
	pub length: u64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Request {
	#[tangram_serialize(id = 0)]
	pub arg: Data,
	#[tangram_serialize(id = 1)]
	pub id: u64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Response {
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,
	#[tangram_serialize(id = 1)]
	pub id: u64,
	#[tangram_serialize(id = 2)]
	pub output: Option<Output>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerMessage {
	#[tangram_serialize(id = 0)]
	Ack(Ack),
	#[tangram_serialize(id = 1)]
	Response(Response),
}

pub(crate) struct Input {
	pub chunk: super::Chunk,
	pub completion: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<O> tg::Process<O> {
	pub async fn write_stdio(
		&self,
		options: tg::process::stdio::write::Options,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.write_stdio_with_handle(handle, options, input).await
	}

	pub async fn write_stdio_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::stdio::write::Options,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let handle = self.handle_with_handle(handle);
		let handle = &handle;
		if self.id().is_left() {
			if options.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
				return Err(tg::error!("writing stdout or stderr is invalid"));
			}
			let mut stdin = self.stdin();
			let mut input = std::pin::pin!(input);
			while let Some(chunk) = input.try_next().await? {
				if chunk.stream != tg::process::stdio::Stream::Stdin {
					return Err(tg::error!("invalid process stdio stream"));
				}
				stdin.write_with_handle(handle, &chunk.bytes).await?;
			}
			stdin.close_with_handle(handle).await?;

			return Ok(());
		}

		if options.location.is_none() && self.location().is_none() {
			self.ensure_location_with_handle(handle).await?;
		}
		let id = self.id().unwrap_right();
		let arg = tg::process::stdio::write::stream::Arg {
			location: options.location.or_else(|| self.location()),
			streams: options.streams,
			tokens: self.tokens(),
		};
		handle.write_process_stdio_all(id, arg, input).await
	}
}

impl tg::Session {
	pub async fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::process::stdio::write::ServerMessage>>
			+ Send
			+ 'static
			+ use<>,
		>,
	> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let max_frame_size = self.client().sync.max_frame_size;
		let path = format!("/processes/{id}/stdio/write");
		let uri = Uri::builder().path(&path).build().unwrap();
		let body = super::encode(input, max_frame_size);
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(uri)
			.header(http::header::ACCEPT, super::TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, super::TANGRAM_CONTENT_TYPE)
			.arg(&arg, body)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
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
		if content_type != Some(super::TANGRAM_CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = super::decode(response.into_body(), max_frame_size);

		Ok(Some(stream))
	}
}

impl TryFrom<ClientMessage> for tangram_http::sse::Event {
	type Error = tg::Error;
	fn try_from(value: ClientMessage) -> tg::Result<Self> {
		let (event, data) = match value {
			ClientMessage::Ack(ack) => ("ack", serde_json::to_string(&ack)),
			ClientMessage::Request(value) => ("request", serde_json::to_string(&value)),
		};
		let data =
			data.map_err(|error| tg::error!(!error, "failed to serialize the stdio message"))?;
		let event = Some(event.to_owned());
		Ok(Self {
			data,
			event,
			..Self::default()
		})
	}
}

impl TryFrom<tangram_http::sse::Event> for ClientMessage {
	type Error = tg::Error;
	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("ack") => {
				let ack = serde_json::from_str(&value.data).map_err(|error| {
					tg::error!(!error, "failed to deserialize the stdio acknowledgment")
				})?;
				Ok(Self::Ack(ack))
			},
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&value.data).map_err(|error| {
						tg::error!(!error, "failed to deserialize the stdio error")
					})?;
				Err(error.try_into()?)
			},
			Some("request") => {
				let value = serde_json::from_str(&value.data).map_err(|error| {
					tg::error!(!error, "failed to deserialize the stdio message")
				})?;
				Ok(Self::Request(value))
			},
			_ => Err(tg::error!("invalid stdio message")),
		}
	}
}

impl TryFrom<ServerMessage> for tangram_http::sse::Event {
	type Error = tg::Error;
	fn try_from(value: ServerMessage) -> tg::Result<Self> {
		let (event, data) = match value {
			ServerMessage::Ack(ack) => ("ack", serde_json::to_string(&ack)),
			ServerMessage::Response(value) => ("response", serde_json::to_string(&value)),
		};
		let data =
			data.map_err(|error| tg::error!(!error, "failed to serialize the stdio message"))?;
		let event = Some(event.to_owned());
		Ok(Self {
			data,
			event,
			..Self::default()
		})
	}
}

impl TryFrom<tangram_http::sse::Event> for ServerMessage {
	type Error = tg::Error;
	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("ack") => {
				let ack = serde_json::from_str(&value.data).map_err(|error| {
					tg::error!(!error, "failed to deserialize the stdio acknowledgment")
				})?;
				Ok(Self::Ack(ack))
			},
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&value.data).map_err(|error| {
						tg::error!(!error, "failed to deserialize the stdio error")
					})?;
				Err(error.try_into()?)
			},
			Some("response") => {
				let value = serde_json::from_str(&value.data).map_err(|error| {
					tg::error!(!error, "failed to deserialize the stdio message")
				})?;
				Ok(Self::Response(value))
			},
			_ => Err(tg::error!("invalid stdio message")),
		}
	}
}
