use {
	crate::prelude::*,
	futures::stream::BoxStream,
	std::time::Duration,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_default,
};

mod connection;

pub use connection::{Connection, Priority, Response, Sender};

pub const TANGRAM_CONTENT_TYPE: &str = "application/vnd.tangram.process-control";

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
	/// The sync that pushes the process's command objects, which a reconnect reuses.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub command_sync: Option<tg::Referent<tg::sync::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[tangram_serialize(id = 2)]
	pub mode: Mode,

	/// The sync that pushes the process's outcome objects, which a reconnect reuses.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub outcome_sync: Option<tg::Referent<tg::sync::Id>>,
}

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Mode {
	#[tangram_serialize(id = 0)]
	Resume {
		#[tangram_serialize(id = 0)]
		lease: tg::process::Lease,
	},
	#[tangram_serialize(id = 1)]
	Start(StartClientRequestArg),
	#[default]
	#[tangram_serialize(id = 2)]
	Wait,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Header {
	/// The sync that pushes the process's command objects.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub command_sync: Option<tg::Referent<tg::sync::Id>>,

	/// The sync that pushes the process's outcome objects.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub outcome_sync: Option<tg::Referent<tg::sync::Id>>,

	#[tangram_serialize(id = 0)]
	pub process: tg::Referent<tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
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
	#[tangram_serialize(id = 1)]
	Ack(ClientAck),

	#[tangram_serialize(id = 2)]
	Notification(ClientNotification),

	#[tangram_serialize(id = 3)]
	Request(ClientRequest),

	#[tangram_serialize(id = 0)]
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
pub struct ClientAck {
	#[tangram_serialize(id = 0)]
	pub id: String,
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
	BorrowableCapacity(BorrowableCapacityClientNotification),

	#[tangram_serialize(id = 1)]
	ChildSpawned,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct BorrowableCapacityClientNotification {
	#[tangram_serialize(id = 4)]
	pub available: bool,

	#[tangram_serialize(id = 0)]
	pub capacity: tg::runner::Capacity,

	#[tangram_serialize(id = 5)]
	pub heartbeat_index: u64,

	#[tangram_serialize(id = 1)]
	pub parent: tg::sandbox::Id,

	#[tangram_serialize(id = 2)]
	pub runner: tg::runner::Id,

	#[tangram_serialize(id = 3)]
	pub scheduler: tg::scheduler::Id,
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
	#[tangram_serialize(id = 2)]
	pub arg: ClientRequestArg,

	#[tangram_serialize(id = 0)]
	pub id: String,
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
	Finish(FinishClientRequestArg),

	#[tangram_serialize(id = 2)]
	Start(StartClientRequestArg),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct FinishClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub data: tg::process::Data,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct StartClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub data: tg::process::Data,

	#[tangram_serialize(id = 1)]
	pub lease: tg::process::Lease,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub options: tg::referent::Options,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	/// The parent's process token, which proves that the runner that starts a process on the shortcut path runs its parent.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub parent_token: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<Sandbox>,

	/// The sandbox's token, which proves that the runner that starts a process on the shortcut path hosts its sandbox.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub sandbox_token: Option<String>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Sandbox {
	#[tangram_serialize(id = 0)]
	pub create: tg::sandbox::create::Arg,

	#[tangram_serialize(id = 1)]
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,

	#[tangram_serialize(id = 3)]
	pub runner: Option<tg::runner::Id>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ClientResponse {
	#[tangram_serialize(id = 1)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 0)]
	pub id: String,

	#[tangram_serialize(id = 2)]
	pub output: Option<ClientResponseOutput>,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientResponseOutput {
	#[tangram_serialize(id = 6)]
	AcquireLease(AcquireLeaseClientResponseOutput),

	#[tangram_serialize(id = 5)]
	Finish(FinishClientResponseOutput),

	#[tangram_serialize(id = 4)]
	Get(GetClientResponseOutput),

	#[tangram_serialize(id = 8)]
	GetChildren(GetChildrenClientResponseOutput),

	#[tangram_serialize(id = 0)]
	Open(OpenClientResponseOutput),

	#[tangram_serialize(id = 7)]
	ReleaseLease(ReleaseLeaseClientResponseOutput),

	#[tangram_serialize(id = 2)]
	Signal(SignalClientResponseOutput),

	#[tangram_serialize(id = 3)]
	Tty(TtyClientResponseOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct AcquireLeaseClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub data: tg::process::Data,

	#[tangram_serialize(id = 1)]
	pub lease: Option<tg::process::Lease>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct FinishClientResponseOutput {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetClientResponseOutput {
	#[tangram_serialize(id = 1)]
	pub data: tg::process::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub sync: Option<tg::Referent<tg::sync::Id>>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetChildrenClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub children: Vec<tg::process::data::Child>,

	#[tangram_serialize(id = 1)]
	pub length: u64,

	#[tangram_serialize(id = 2)]
	pub status: tg::process::Status,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct OpenClientResponseOutput {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReleaseLeaseClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub released: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct SignalClientResponseOutput {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct TtyClientResponseOutput {}

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
	#[tangram_serialize(id = 1)]
	Ack(ServerAck),

	#[tangram_serialize(id = 2)]
	Notification(ServerNotification),

	#[tangram_serialize(id = 0)]
	Request(ServerRequest),

	#[tangram_serialize(id = 3)]
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
pub struct ServerAck {
	#[tangram_serialize(id = 0)]
	pub id: String,
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
	Sync(SyncServerNotification),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct SyncServerNotification {
	#[tangram_serialize(id = 0)]
	pub command_sync: tg::Referent<tg::sync::Id>,

	#[tangram_serialize(id = 1)]
	pub outcome_sync: tg::Referent<tg::sync::Id>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ServerRequest {
	#[tangram_serialize(id = 2)]
	pub arg: ServerRequestArg,

	#[tangram_serialize(id = 0)]
	pub id: String,
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
pub enum ServerRequestArg {
	#[tangram_serialize(id = 6)]
	AcquireLease(AcquireLeaseServerRequestArg),

	#[tangram_serialize(id = 5)]
	Finish(FinishServerRequestArg),

	#[tangram_serialize(id = 4)]
	Get(GetServerRequestArg),

	#[tangram_serialize(id = 8)]
	GetChildren(GetChildrenServerRequestArg),

	#[tangram_serialize(id = 0)]
	Open(OpenServerRequestArg),

	#[tangram_serialize(id = 7)]
	ReleaseLease(ReleaseLeaseServerRequestArg),

	#[tangram_serialize(id = 2)]
	Signal(SignalServerRequestArg),

	#[tangram_serialize(id = 3)]
	Tty(TtyServerRequestArg),
}

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct AcquireLeaseServerRequestArg {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct FinishServerRequestArg {
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub exit: u8,
}

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetServerRequestArg {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetChildrenServerRequestArg {
	#[tangram_serialize(id = 1)]
	pub length: u64,

	#[tangram_serialize(id = 0)]
	pub position: u64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct OpenServerRequestArg {
	#[tangram_serialize(id = 0)]
	pub arg: tg::process::stdio::read::Arg,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReleaseLeaseServerRequestArg {
	#[tangram_serialize(id = 0)]
	pub lease: tg::process::Lease,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct SignalServerRequestArg {
	#[tangram_serialize(id = 1, display, from_str)]
	pub signal: tg::process::signal::Signal,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct TtyServerRequestArg {
	#[tangram_serialize(id = 1)]
	pub size: tg::process::tty::Size,
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
	#[tangram_serialize(id = 1)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 0)]
	pub id: String,

	#[tangram_serialize(id = 2)]
	pub output: Option<ServerResponseOutput>,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerResponseOutput {
	#[tangram_serialize(id = 0)]
	Finish(FinishServerResponseOutput),

	#[tangram_serialize(id = 2)]
	Start(StartServerResponseOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct FinishServerResponseOutput {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct StartServerResponseOutput {}

#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub event_capacity: usize,
	pub inbox_ttl: Duration,
	pub max_deliveries: usize,
	pub max_untracked_messages: usize,
	pub message_capacity: usize,
	pub resend_interval: Duration,
}

impl Config {
	pub fn validate(self) -> tg::Result<Self> {
		if self.event_capacity == 0 {
			return Err(tg::error!(
				"expected the event capacity to be greater than zero"
			));
		}
		if self.inbox_ttl.is_zero() {
			return Err(tg::error!(
				"expected the inbox time to live to be greater than zero"
			));
		}
		if self.max_deliveries == 0 {
			return Err(tg::error!(
				"expected the maximum number of deliveries to be greater than zero"
			));
		}
		if self.max_untracked_messages == 0 {
			return Err(tg::error!(
				"expected the maximum number of untracked messages to be greater than zero"
			));
		}
		if self.message_capacity == 0 {
			return Err(tg::error!(
				"expected the message capacity to be greater than zero"
			));
		}
		if self.resend_interval.is_zero() {
			return Err(tg::error!(
				"expected the resend interval to be greater than zero"
			));
		}
		Ok(self)
	}
}

impl tg::Session {
	pub async fn try_get_process_control_stream(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			impl futures::Stream<Item = tg::Result<tg::process::control::ServerMessage>>
			+ Send
			+ 'static
			+ use<>,
		)>,
	> {
		let method = http::Method::POST;
		let path = "/processes/control";
		let uri = Uri::builder().path(path).build().unwrap();
		let max_frame_size = self.client().sync.max_frame_size;
		let body = super::stdio::encode(stream, max_frame_size);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE)
			.header(tangram_http::body::arg::HEADER, "true")
			.arg_with_tangram(&arg, body)
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
		if content_type != Some(TANGRAM_CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let (header, stream) = super::stdio::decode_with_header(
			response.into_body(),
			self.client().http.max_header_length,
			max_frame_size,
		)
		.await?;
		Ok(Some((header, stream)))
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			event_capacity: 64,
			inbox_ttl: Duration::from_mins(1),
			max_deliveries: 64,
			max_untracked_messages: 256,
			message_capacity: 256,
			resend_interval: Duration::from_secs(1),
		}
	}
}

impl TryFrom<ServerMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: ServerMessage) -> Result<Self, Self::Error> {
		let event = match value {
			ServerMessage::Ack(ack) => {
				let data = serde_json::to_string(&ack)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("ack".to_owned()),
					..Default::default()
				}
			},
			ServerMessage::Notification(notification) => {
				let data = serde_json::to_string(&notification)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("notification".to_owned()),
					..Default::default()
				}
			},
			ServerMessage::Request(request) => {
				let data = serde_json::to_string(&request)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("request".to_owned()),
					..Default::default()
				}
			},
			ServerMessage::Response(response) => {
				let data = serde_json::to_string(&response)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
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
			Some("ack") => {
				let ack = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Ack(ack))
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
			Some("response") => {
				let response = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Response(response))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid message")),
		}
	}
}

impl TryFrom<ClientMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: ClientMessage) -> Result<Self, Self::Error> {
		let event = match value {
			ClientMessage::Ack(ack) => {
				let data = serde_json::to_string(&ack)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("ack".to_owned()),
					..Default::default()
				}
			},
			ClientMessage::Notification(notification) => {
				let data = serde_json::to_string(&notification)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("notification".to_owned()),
					..Default::default()
				}
			},
			ClientMessage::Request(request) => {
				let data = serde_json::to_string(&request)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("request".to_owned()),
					..Default::default()
				}
			},
			ClientMessage::Response(response) => {
				let data = serde_json::to_string(&response)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
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
			Some("ack") => {
				let ack = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Ack(ack))
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
			Some("response") => {
				let response = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok(Self::Response(response))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid message")),
		}
	}
}
