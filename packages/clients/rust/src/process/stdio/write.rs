use {
	super::Stream,
	crate::prelude::*,
	futures::{TryStreamExt as _, stream::BoxStream},
	serde_with::serde_as,
	tangram_http::response::Ext as _,
	tangram_uri::Uri,
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[serde_as(as = "CommaSeparatedString")]
	pub streams: Vec<Stream>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
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
	Notification(ClientNotification),

	#[tangram_serialize(id = 1)]
	Request(ClientRequest),
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
pub enum ClientNotification {
	#[tangram_serialize(id = 0)]
	Chunk(super::Chunk),
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
pub enum ClientRequest {
	#[tangram_serialize(id = 0)]
	End,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
	pub streams: Vec<Stream>,
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
	Notification(ServerNotification),

	#[tangram_serialize(id = 1)]
	Response(ServerResponse),
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
pub enum ServerNotification {
	#[tangram_serialize(id = 0)]
	Stop,

	#[tangram_serialize(id = 1)]
	Write { position: u64 },
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
pub enum ServerResponse {
	#[tangram_serialize(id = 0)]
	End,
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
		let arg = tg::process::stdio::write::Arg {
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
		arg: tg::process::stdio::write::Arg,
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
		let uri = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let body = super::encode(input, max_frame_size);
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(uri)
			.header(http::header::ACCEPT, super::TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, super::TANGRAM_CONTENT_TYPE)
			.body(body)
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
		let event = match value {
			ClientMessage::Notification(notification) => {
				let data = serde_json::to_string(&notification)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				Self {
					data,
					event: Some("notification".to_owned()),
					..Default::default()
				}
			},
			ClientMessage::Request(request) => {
				let data = serde_json::to_string(&request)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				Self {
					data,
					event: Some("request".to_owned()),
					..Default::default()
				}
			},
		};

		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ClientMessage {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&value.data)
						.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
				let error = error.try_into()?;
				Err(error)
			},
			Some("notification") => {
				let notification = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Notification(notification))
			},
			Some("request") => {
				let request = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Request(request))
			},
			_ => Err(tg::error!("invalid message")),
		}
	}
}

impl TryFrom<ServerMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: ServerMessage) -> tg::Result<Self> {
		let event = match value {
			ServerMessage::Notification(notification) => {
				let data = serde_json::to_string(&notification)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				Self {
					data,
					event: Some("notification".to_owned()),
					..Default::default()
				}
			},
			ServerMessage::Response(response) => {
				let data = serde_json::to_string(&response)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				Self {
					data,
					event: Some("response".to_owned()),
					..Default::default()
				}
			},
		};

		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ServerMessage {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&value.data)
						.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
				let error = error.try_into()?;
				Err(error)
			},
			Some("notification") => {
				let notification = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Notification(notification))
			},
			Some("response") => {
				let response = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Response(response))
			},
			_ => Err(tg::error!("invalid message")),
		}
	}
}
