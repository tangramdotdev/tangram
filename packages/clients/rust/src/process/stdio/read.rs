use {
	super::{Chunk, Stream},
	crate::prelude::*,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_http::response::Ext as _,
	tangram_uri::Uri,
	tangram_util::serde::{CommaSeparatedString, SeekFromNumberOrString},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromNumberOrString>")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[serde_as(as = "CommaSeparatedString")]
	pub streams: Vec<Stream>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub timeout: Option<Duration>,

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
	Response(ClientResponse),
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
	Read { position: u64 },
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
pub enum ClientResponse {
	#[tangram_serialize(id = 0)]
	End,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub length: Option<i64>,
	pub location: Option<tg::location::Arg>,
	pub position: Option<std::io::SeekFrom>,
	pub size: Option<u64>,
	pub streams: Vec<Stream>,
	pub timeout: Option<Duration>,
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
	Request(ServerRequest),
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
	Chunk(Chunk),

	#[tangram_serialize(id = 1)]
	Stop,
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
pub enum ServerRequest {
	#[tangram_serialize(id = 0)]
	End,
}

impl<O> tg::Process<O> {
	pub async fn try_read_stdio(
		&self,
		options: tg::process::stdio::read::Options,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>> {
		let handle = tg::handle()?;
		self.try_read_stdio_with_handle(handle, options).await
	}

	pub async fn try_read_stdio_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::stdio::read::Options,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>>
	where
		H: tg::Handle,
	{
		if options.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		if self.id().is_left() {
			let mut streams = Vec::<
				BoxStream<'static, tg::Result<(Bytes, tg::process::stdio::Stream, u64)>>,
			>::new();
			for stream in options.streams {
				match stream {
					tg::process::stdio::Stream::Stdin => {
						return Err(tg::error!("reading stdin is invalid"));
					},
					tg::process::stdio::Stream::Stdout => {
						let handle = handle.clone();
						let stdout = self.stdout();
						let stream = stream::try_unfold(
							(handle, stdout, 0),
							|(handle, mut stdout, stream_position)| async move {
								let Some(bytes) = stdout.read_with_handle(&handle).await? else {
									return Ok(None);
								};
								let length = bytes.len().to_u64().unwrap();
								let item =
									(bytes, tg::process::stdio::Stream::Stdout, stream_position);
								let stream_position = stream_position + length;

								Ok(Some((item, (handle, stdout, stream_position))))
							},
						);
						streams.push(stream.boxed());
					},
					tg::process::stdio::Stream::Stderr => {
						let handle = handle.clone();
						let stderr = self.stderr();
						let stream = stream::try_unfold(
							(handle, stderr, 0),
							|(handle, mut stderr, stream_position)| async move {
								let Some(bytes) = stderr.read_with_handle(&handle).await? else {
									return Ok(None);
								};
								let length = bytes.len().to_u64().unwrap();
								let item =
									(bytes, tg::process::stdio::Stream::Stderr, stream_position);
								let stream_position = stream_position + length;

								Ok(Some((item, (handle, stderr, stream_position))))
							},
						);
						streams.push(stream.boxed());
					},
				}
			}
			let stream = futures::stream::select_all(streams).boxed();
			let stream =
				stream::try_unfold((stream, 0), |(mut stream, combined_position)| async move {
					let Some((bytes, stream_name, stream_position)) = stream.try_next().await?
					else {
						return Ok(None);
					};
					let length = bytes.len().to_u64().unwrap();
					let chunk = tg::process::stdio::Chunk {
						bytes,
						combined_position,
						stream: stream_name,
						stream_position,
						timestamp: None,
					};
					let combined_position = combined_position + length;

					Ok(Some((chunk, (stream, combined_position))))
				});

			return Ok(Some(stream.boxed()));
		}

		let id = self.id().unwrap_right();
		let arg = tg::process::stdio::read::Arg {
			length: options.length,
			location: options.location.or_else(|| self.location()),
			position: options.position,
			size: options.size,
			streams: options.streams,
			timeout: options.timeout,
			tokens: self.tokens(),
		};
		let Some(stream) = handle.try_read_process_stdio_all(id, arg).await? else {
			return Ok(None);
		};

		Ok(Some(stream.boxed()))
	}
}

impl tg::Session {
	pub async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::process::stdio::read::ServerMessage>>
			+ Send
			+ 'static
			+ use<>,
		>,
	> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let max_frame_size = self.client().sync.max_frame_size;
		let path = format!("/processes/{id}/stdio/read");
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
			ClientMessage::Response(response) => {
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
			Some("response") => {
				let response = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Response(response))
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
			ServerMessage::Request(request) => {
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
			Some("request") => {
				let request = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Request(request))
			},
			_ => Err(tg::error!("invalid message")),
		}
	}
}
