//! A parent connection selects a process once and multiplexes its lifecycle operations.
//!
//! The first request is `Connect`. Spawn mode returns the selected process and closes;
//! Run mode also starts waiting and the reads declared in the opening argument.
//! Each stdio subscription retains the existing read/write protocol, scoped by its ID.
//! An acknowledgment confirms receipt; only a response confirms an operation outcome.
//! Completion does not imply stdio EOF. The server drains subscribed reads before closing.
//! Normal closure also waits for receipt acknowledgments of outstanding responses.
//! A disconnected leased wait cancels the process. Detach must receive its response first.

use {
	crate::prelude::*,
	futures::{StreamExt as _, stream::BoxStream},
	tangram_http::response::Ext as _,
};

mod connection;
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
pub struct Arg {
	#[serde(default)]
	#[tangram_serialize(default, id = 0)]
	pub reads: std::collections::BTreeMap<u64, tg::process::stdio::read::Arg>,
	#[tangram_serialize(id = 1)]
	pub target: Target,
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
pub enum Target {
	#[tangram_serialize(id = 0)]
	Existing {
		id: tg::process::Id,
		#[serde(flatten)]
		options: tg::process::wait::Arg,
	},
	#[tangram_serialize(id = 1)]
	Spawn {
		arg: Box<tg::process::spawn::Arg>,
		#[serde(default)]
		mode: Mode,
	},
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
	Notification(ClientNotification),
	#[tangram_serialize(id = 2)]
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
pub enum ServerMessage {
	#[tangram_serialize(id = 0)]
	Ack(Ack),
	#[tangram_serialize(id = 1)]
	Notification(ServerNotification),
	#[tangram_serialize(id = 2)]
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
	Read,
	#[tangram_serialize(id = 5)]
	Signal,
	#[tangram_serialize(id = 6)]
	Tty,
	#[tangram_serialize(id = 7)]
	Write,
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
	#[tangram_serialize(id = 1)]
	Write(WriteClientNotification),
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
	#[tangram_serialize(id = 4)]
	Error(ErrorServerNotification),

	#[tangram_serialize(id = 0)]
	Progress(tg::progress::Event<()>),
	#[tangram_serialize(id = 1)]
	Read(ReadServerNotification),
	#[tangram_serialize(id = 2)]
	Wait(tg::process::wait::Output),
	#[tangram_serialize(id = 3)]
	Write(WriteServerNotification),
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
	pub message: tg::process::stdio::read::ClientMessage,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct WriteClientNotification {
	#[tangram_serialize(id = 0)]
	pub id: u64,
	#[tangram_serialize(id = 1)]
	pub message: tg::process::stdio::write::ClientMessage,
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
	#[tangram_serialize(id = 0)]
	pub id: u64,
	#[tangram_serialize(id = 1)]
	pub message: tg::process::stdio::read::ServerMessage,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct WriteServerNotification {
	#[tangram_serialize(id = 0)]
	pub id: u64,
	#[tangram_serialize(id = 1)]
	pub message: tg::process::stdio::write::ServerMessage,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ErrorServerNotification {
	#[tangram_serialize(id = 0)]
	pub error: tg::error::Data,
	#[tangram_serialize(id = 1)]
	pub id: u64,
}

impl tg::Session {
	pub async fn connect_process(
		&self,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<ServerMessage>>> {
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
		Ok(stream.boxed())
	}
}

impl TryFrom<ClientMessage> for tangram_http::sse::Event {
	type Error = tg::Error;
	fn try_from(message: ClientMessage) -> tg::Result<Self> {
		let (event, data) = match message {
			ClientMessage::Ack(value) => ("ack", serde_json::to_string(&value)),
			ClientMessage::Notification(value) => ("notification", serde_json::to_string(&value)),
			ClientMessage::Request(value) => ("request", serde_json::to_string(&value)),
		};
		let data = data.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
		Ok(Self {
			data,
			event: Some(event.to_owned()),
			..Default::default()
		})
	}
}

impl TryFrom<tangram_http::sse::Event> for ClientMessage {
	type Error = tg::Error;
	fn try_from(event: tangram_http::sse::Event) -> tg::Result<Self> {
		let message = match event.event.as_deref() {
			Some("ack") => serde_json::from_str(&event.data).map(Self::Ack),
			Some("notification") => serde_json::from_str(&event.data).map(Self::Notification),
			Some("request") => serde_json::from_str(&event.data).map(Self::Request),
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
		};
		let data = data.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
		Ok(Self {
			data,
			event: Some(event.to_owned()),
			..Default::default()
		})
	}
}

impl TryFrom<tangram_http::sse::Event> for ServerMessage {
	type Error = tg::Error;
	fn try_from(event: tangram_http::sse::Event) -> tg::Result<Self> {
		let message = match event.event.as_deref() {
			Some("ack") => serde_json::from_str(&event.data).map(Self::Ack),
			Some("notification") => serde_json::from_str(&event.data).map(Self::Notification),
			Some("response") => serde_json::from_str(&event.data).map(Self::Response),
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
		use tangram_futures::stream::TryExt as _;
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
					tokens: tg::authorization::Tokens::default(),
				};
				(index as u64 + 1, arg)
			})
			.collect();
		let wait = tg::process::wait::Arg {
			lease: options.lease.clone(),
			location: options.location.clone(),
			tokens: options.tokens.clone(),
		};
		let arg = Arg {
			reads,
			target: Target::Existing {
				id: id.clone(),
				options: wait,
			},
		};
		let (connection, progress) = Connection::open(handle, arg).await?;
		progress
			.try_last()
			.await?
			.ok_or_else(|| tg::error!("missing the connect output"))?;
		let options = tg::process::Options {
			lease: options.lease,
			location: options.location,
			tokens: options.tokens,
			..Default::default()
		};
		let handle = tg::handle::dynamic::Handle::new(handle.clone());
		let process = Self::new_inner(id, options, Some(handle), Some(connection));
		Ok(process)
	}
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
