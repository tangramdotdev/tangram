//! A parent connection selects a process once and multiplexes its lifecycle operations.
//!
//! The first request is `Connect`. Spawn mode returns the selected process and closes;
//! Run mode also starts waiting and the reads declared in the opening argument.
//! Each stdio read request retains the shared read protocol, scoped by its request ID.
//! An acknowledgment confirms receipt; only a response confirms an operation outcome.
//! Completion does not imply stdio EOF. The server drains open reads before closing.
//! Normal closure also waits for receipt acknowledgments of outstanding responses.
//! A disconnected leased wait cancels the process. Detach must receive its response first.
//! Subsequent operations reopen the selected process ID through one shared connection.
//! Reads resume at their cursor; writes resend only requests without a completed outcome.

use {
	crate::prelude::*,
	futures::{StreamExt as _, stream::BoxStream},
	tangram_futures::stream::TryExt as _,
	tangram_http::response::Ext as _,
	tangram_util::serde::is_false,
};

mod connection;
mod session;
#[cfg(test)]
mod tests;

pub use connection::Connection;

pub const TANGRAM_CONTENT_TYPE: &str = "application/vnd.tangram.process-connect";

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
	Notification(ClientNotification),
	#[tangram_serialize(id = 2)]
	Request(ClientRequest),
	#[tangram_serialize(id = 3)]
	Sync(Vec<u8>),
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
	Notification(ServerNotification),
	#[tangram_serialize(id = 2)]
	Response(ServerResponse),
	#[tangram_serialize(id = 3)]
	Sync(Vec<u8>),
}

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
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientNotification {
	#[tangram_serialize(id = 0)]
	Read(ReadClientNotification),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ClientRequest {
	#[tangram_serialize(id = 0)]
	pub arg: ClientRequestArg,
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
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientRequestArg {
	#[tangram_serialize(id = 0)]
	Cancel(tg::process::cancel::Arg),
	#[tangram_serialize(id = 1)]
	Close(u64),
	#[tangram_serialize(id = 2)]
	Connect(Arg),
	#[tangram_serialize(id = 3)]
	Detach,
	#[tangram_serialize(id = 4)]
	Read(tg::process::stdio::read::Arg),
	#[tangram_serialize(id = 5)]
	Signal(tg::process::signal::post::Arg),
	#[tangram_serialize(id = 6)]
	Tty(tg::process::tty::size::put::Arg),
	#[tangram_serialize(id = 7)]
	Write(tg::process::stdio::write::Arg),
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
	Progress(tg::progress::Event<()>),
	#[tangram_serialize(id = 1)]
	Read(ReadServerNotification),
	#[tangram_serialize(id = 2)]
	Wait(tg::process::wait::Output),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ServerResponse {
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,
	#[tangram_serialize(id = 1)]
	pub id: u64,
	#[tangram_serialize(id = 2)]
	pub output: Option<ServerResponseOutput>,
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
pub enum ServerResponseOutput {
	#[tangram_serialize(id = 0)]
	Cancel(tg::process::cancel::Output),
	#[tangram_serialize(id = 1)]
	Close,
	#[tangram_serialize(id = 2)]
	Connect(tg::process::spawn::Output),
	#[tangram_serialize(id = 3)]
	Detach,
	#[tangram_serialize(id = 4)]
	Read(tg::process::stdio::read::Output),
	#[tangram_serialize(id = 5)]
	Signal,
	#[tangram_serialize(id = 6)]
	Tty,
	#[tangram_serialize(id = 7)]
	Write(tg::process::stdio::write::Output),
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
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "is_false")]
	pub command_sync: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub lease: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default)]
	#[tangram_serialize(default, id = 2)]
	pub mode: Mode,

	#[serde(deserialize_with = "deserialize_process")]
	#[tangram_serialize(id = 3)]
	pub process: tg::Either<Box<tg::process::spawn::Arg>, tg::process::Id>,

	#[serde(default)]
	#[tangram_serialize(default, id = 4)]
	pub reads: std::collections::BTreeMap<u64, tg::process::stdio::read::Arg>,

	#[serde(default, skip_serializing_if = "tg::Tokens::is_empty")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "tg::Tokens::is_empty")]
	pub tokens: tg::Tokens,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
	#[tangram_serialize(id = 0)]
	Run,
	#[default]
	#[tangram_serialize(id = 1)]
	Spawn,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub lease: Option<String>,
	pub location: Option<tg::location::Arg>,
	pub reads: Vec<tg::process::stdio::read::Options>,
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
pub struct ReadClientNotification {
	#[tangram_serialize(id = 0)]
	pub id: u64,
	#[tangram_serialize(id = 1)]
	pub progress: tg::process::stdio::read::Progress,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReadServerNotification {
	#[tangram_serialize(id = 1)]
	pub event: tg::process::stdio::read::Event,
	#[tangram_serialize(id = 0)]
	pub id: u64,
}

pub async fn connect(id: tg::process::Id, options: Options) -> tg::Result<tg::Process> {
	tg::Process::connect(id, options).await
}

pub async fn connect_with_handle<H: tg::Handle>(
	handle: &H,
	id: tg::process::Id,
	options: Options,
) -> tg::Result<tg::Process> {
	tg::Process::connect_with_handle(handle, id, options).await
}

impl<O: 'static> tg::Process<O> {
	pub(super) async fn connect_spawn_with_progress_with_handle<H, F, Fut>(
		handle: &H,
		arg: tg::process::Arg,
		mode: Mode,
		progress: F,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
		F: FnOnce(
			BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		) -> Fut,
		Fut: Future<Output = tg::Result<tg::process::spawn::Output>>,
	{
		let arg = super::spawn::spawn_arg_with_handle(handle, arg).await?;
		let options = tg::process::spawn::Options { mode };
		Self::spawn_inner_with_handle(handle, arg, options, progress).await
	}

	pub async fn connect(id: tg::process::Id, options: Options) -> tg::Result<Self> {
		let handle = tg::handle()?;
		Self::connect_with_handle(handle, id, options).await
	}

	pub async fn connect_with_handle<H: tg::Handle>(
		handle: &H,
		id: tg::process::Id,
		options: Options,
	) -> tg::Result<Self> {
		let reads = options
			.reads
			.into_iter()
			.enumerate()
			.map(|(index, options)| {
				let arg = tg::process::stdio::read::Arg {
					length: options.length,
					location: options.location,
					position: options.position,
					size: options.size,
					streams: options.streams,
					timeout: options.timeout,
					tokens: tg::Tokens::default(),
				};
				(index as u64 + 1, arg)
			})
			.collect();
		let arg = Arg {
			command_sync: false,
			lease: options.lease,
			location: options.location,
			mode: Mode::Run,
			process: tg::Either::Right(id.clone()),
			reads,
			tokens: options.tokens,
		};
		let (connection, progress) = Connection::open(handle, arg).await?;
		let output = progress
			.try_last()
			.await?
			.ok_or_else(|| tg::error!("missing the connect output"))?
			.unwrap_output();
		let options = tg::process::Options {
			lease: output.lease,
			location: output.location.map(Into::into),
			tokens: output.tokens,
			..Default::default()
		};
		let handle = tg::handle::dynamic::Handle::new(handle.clone());
		let process = Self::new_inner(id, options, Some(handle), Some(connection));
		Ok(process)
	}
}

impl tg::Session {
	pub async fn try_connect_process(
		&self,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<ServerMessage>>>> {
		let max_frame_size = self.client().sync.max_frame_size;
		let body = super::stdio::encode(input, max_frame_size);
		let request = http::Request::builder()
			.method(http::Method::POST)
			.uri("/processes/connect")
			.header(http::header::ACCEPT, TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE)
			.body(body)
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to connect to the process"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			return Err(error);
		}
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if content_type != Some(TANGRAM_CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = super::stdio::decode(response.into_body(), max_frame_size);
		Ok(Some(stream.boxed()))
	}
}

impl TryFrom<ClientMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(message: ClientMessage) -> tg::Result<Self> {
		let (event, data) = match message {
			ClientMessage::Ack(value) => ("ack", serde_json::to_string(&value)),
			ClientMessage::Notification(value) => ("notification", serde_json::to_string(&value)),
			ClientMessage::Request(value) => ("request", serde_json::to_string(&value)),
			ClientMessage::Sync(value) => ("sync", serde_json::to_string(&value)),
		};
		let data = data.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
		let event = Some(event.to_owned());
		let event = Self {
			data,
			event,
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ClientMessage {
	type Error = tg::Error;

	fn try_from(event: tangram_http::sse::Event) -> tg::Result<Self> {
		let message = match event.event.as_deref() {
			Some("ack") => serde_json::from_str(&event.data).map(Self::Ack),
			Some("notification") => serde_json::from_str(&event.data).map(Self::Notification),
			Some("request") => serde_json::from_str(&event.data).map(Self::Request),
			Some("sync") => serde_json::from_str(&event.data).map(Self::Sync),
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&event.data)
						.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
				return Err(error.try_into()?);
			},
			_ => return Err(tg::error!("invalid process connect message")),
		}
		.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
		Ok(message)
	}
}

impl TryFrom<ServerMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(message: ServerMessage) -> tg::Result<Self> {
		let (event, data) = match message {
			ServerMessage::Ack(value) => ("ack", serde_json::to_string(&value)),
			ServerMessage::Notification(value) => ("notification", serde_json::to_string(&value)),
			ServerMessage::Response(value) => ("response", serde_json::to_string(&value)),
			ServerMessage::Sync(value) => ("sync", serde_json::to_string(&value)),
		};
		let data = data.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
		let event = Some(event.to_owned());
		let event = Self {
			data,
			event,
			..Default::default()
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for ServerMessage {
	type Error = tg::Error;

	fn try_from(event: tangram_http::sse::Event) -> tg::Result<Self> {
		let message = match event.event.as_deref() {
			Some("ack") => serde_json::from_str(&event.data).map(Self::Ack),
			Some("notification") => serde_json::from_str(&event.data).map(Self::Notification),
			Some("response") => serde_json::from_str(&event.data).map(Self::Response),
			Some("sync") => serde_json::from_str(&event.data).map(Self::Sync),
			Some("error") => {
				let error: tg::Either<tg::error::Data, tg::error::Id> =
					serde_json::from_str(&event.data)
						.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
				return Err(error.try_into()?);
			},
			_ => return Err(tg::error!("invalid process connect message")),
		}
		.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
		Ok(message)
	}
}

fn deserialize_process<'de, D>(
	deserializer: D,
) -> Result<tg::Either<Box<tg::process::spawn::Arg>, tg::process::Id>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	struct Visitor;

	impl<'de> serde::de::Visitor<'de> for Visitor {
		type Value = tg::Either<Box<tg::process::spawn::Arg>, tg::process::Id>;

		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("a process spawn argument or process ID")
		}

		fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
		where
			A: serde::de::MapAccess<'de>,
		{
			let deserializer = serde::de::value::MapAccessDeserializer::new(map);
			let arg = serde::Deserialize::deserialize(deserializer)?;
			let arg = Box::new(arg);
			Ok(tg::Either::Left(arg))
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			let id = value.parse().map_err(E::custom)?;
			Ok(tg::Either::Right(id))
		}

		fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			self.visit_str(&value)
		}
	}

	deserializer.deserialize_any(Visitor)
}
