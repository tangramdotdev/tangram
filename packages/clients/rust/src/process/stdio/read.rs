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
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{CommaSeparatedString, SeekFromNumberOrString},
};

#[serde_as]
#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub length: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<SeekFromNumberOrString>")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub position: Option<std::io::SeekFrom>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub size: Option<u64>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[serde_as(as = "CommaSeparatedString")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Vec::is_empty")]
	pub streams: Vec<Stream>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Duration>,

	#[serde(default, skip_serializing_if = "tg::Tokens::is_empty")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "tg::Tokens::is_empty")]
	pub tokens: tg::Tokens,
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
	Ack,
	#[tangram_serialize(id = 1)]
	Notification(Progress),
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
	Notification(Event),
	#[tangram_serialize(id = 1)]
	Response(Output),
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
pub enum Event {
	#[tangram_serialize(id = 0)]
	Chunk(Chunk),
	/// The resolved cursor and remaining length, including any clipping at EOF.
	#[tangram_serialize(id = 1)]
	Position {
		#[tangram_serialize(id = 0)]
		length: Option<i64>,
		#[tangram_serialize(id = 1)]
		position: u64,
	},
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
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Output {
	#[tangram_serialize(id = 0)]
	End(super::End),
	#[tangram_serialize(id = 1)]
	Limit {
		#[tangram_serialize(id = 0)]
		position: u64,
	},
	#[tangram_serialize(id = 2)]
	Timeout {
		#[tangram_serialize(id = 0)]
		position: u64,
	},
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Progress {
	/// The cumulative number of consumed chunk bytes in this read attempt.
	#[tangram_serialize(id = 0)]
	pub consumed: u64,
}

impl Output {
	pub fn validate(&self, streams: &[Stream], position: u64) -> tg::Result<()> {
		let expected = self.position(streams)?;
		let valid = match self {
			// A forward read may begin beyond EOF, but it must not finish short of EOF.
			Self::End(_) => position >= expected,
			Self::Limit { .. } | Self::Timeout { .. } => position == expected,
		};
		if !valid {
			return Err(
				tg::error!(expected = %expected, actual = %position, "encountered a gap at the end of the stdio read"),
			);
		}
		Ok(())
	}

	pub fn position(&self, streams: &[Stream]) -> tg::Result<u64> {
		let position = match self {
			Self::End(end) if streams.len() > 1 => end.combined_position,
			Self::End(end) => *streams
				.first()
				.and_then(|stream| end.stream_positions.get(stream))
				.ok_or_else(|| tg::error!("missing the stdio end position"))?,
			Self::Limit { position } | Self::Timeout { position } => *position,
		};
		Ok(position)
	}
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
		let handle = self.handle_with_handle(handle);
		let handle = &handle;
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
	fn try_from(message: ClientMessage) -> tg::Result<Self> {
		let (event, data) = match message {
			ClientMessage::Ack => ("ack", Ok("null".to_owned())),
			ClientMessage::Notification(progress) => {
				("notification", serde_json::to_string(&progress))
			},
		};
		let data = data.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
		let event = Self {
			data,
			event: Some(event.to_owned()),
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ClientMessage {
	type Error = tg::Error;
	fn try_from(event: tangram_http::sse::Event) -> tg::Result<Self> {
		let message = match event.event.as_deref() {
			Some("ack") => serde_json::from_str::<()>(&event.data).map(|()| Self::Ack),
			Some("notification") => serde_json::from_str(&event.data).map(Self::Notification),
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&event.data)
						.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
				return Err(error.try_into()?);
			},
			_ => return Err(tg::error!("invalid stdio read message")),
		}
		.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
		Ok(message)
	}
}

impl TryFrom<ServerMessage> for tangram_http::sse::Event {
	type Error = tg::Error;
	fn try_from(message: ServerMessage) -> tg::Result<Self> {
		let (event, data) = match message {
			ServerMessage::Notification(event) => ("notification", serde_json::to_string(&event)),
			ServerMessage::Response(output) => ("response", serde_json::to_string(&output)),
		};
		let data = data.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
		let event = Self {
			data,
			event: Some(event.to_owned()),
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ServerMessage {
	type Error = tg::Error;
	fn try_from(event: tangram_http::sse::Event) -> tg::Result<Self> {
		let message = match event.event.as_deref() {
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&event.data)
						.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
				return Err(error.try_into()?);
			},
			Some("notification") => serde_json::from_str(&event.data).map(Self::Notification),
			Some("response") => serde_json::from_str(&event.data).map(Self::Response),
			_ => return Err(tg::error!("invalid stdio read message")),
		}
		.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
		Ok(message)
	}
}
