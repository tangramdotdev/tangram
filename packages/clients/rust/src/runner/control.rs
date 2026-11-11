use {
	crate::prelude::*,
	futures::stream::BoxStream,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::{collections::BTreeSet, ops::ControlFlow, time::Duration},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_default,
};

pub const TANGRAM_CONTENT_TYPE: &str = "application/vnd.tangram.runner-control";

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
	Ack(ClientAck),

	#[tangram_serialize(id = 1)]
	Notification(ClientNotification),

	#[tangram_serialize(id = 2)]
	Request(ClientRequest),

	#[tangram_serialize(id = 3)]
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
pub enum ServerMessage {
	#[tangram_serialize(id = 0)]
	Ack(ServerAck),

	#[tangram_serialize(id = 1)]
	Request(ServerRequest),

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
	Heartbeat(HeartbeatClientNotification),

	#[tangram_serialize(id = 1)]
	SandboxEnded(SandboxEndedClientNotification),
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
	DestroySandbox(DestroySandboxClientRequestArg),

	#[tangram_serialize(id = 2)]
	ReportSandboxUsage(ReportSandboxUsageClientRequestArg),

	#[tangram_serialize(id = 1)]
	StopSandbox(StopSandboxClientRequestArg),
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
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
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
	#[tangram_serialize(id = 0)]
	CreateSandbox(CreateSandboxClientResponseOutput),

	#[tangram_serialize(id = 1)]
	DestroySandbox(DestroySandboxClientResponseOutput),
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
pub struct ServerRequest {
	#[tangram_serialize(id = 0)]
	pub arg: ServerRequestArg,

	#[tangram_serialize(id = 1)]
	pub id: String,
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
pub enum ServerRequestArg {
	#[tangram_serialize(id = 0)]
	CreateSandbox(CreateSandboxServerRequestArg),

	#[tangram_serialize(id = 1)]
	DestroySandbox(DestroySandboxServerRequestArg),
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
	pub id: String,

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
	DestroySandbox(DestroySandboxServerResponseOutput),

	#[tangram_serialize(id = 2)]
	ReportSandboxUsage(ReportSandboxUsageServerResponseOutput),

	#[tangram_serialize(id = 1)]
	StopSandbox(StopSandboxServerResponseOutput),
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
pub struct HeartbeatClientNotification {
	#[tangram_serialize(id = 0)]
	pub capacity: Capacity,

	#[tangram_serialize(id = 1)]
	pub index: u64,

	#[tangram_serialize(id = 2)]
	pub sandboxes: BTreeSet<tg::sandbox::Id>,
}

/// A report that a boot of a sandbox ended on the runner.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct SandboxEndedClientNotification {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,
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
pub struct Capacity {
	#[tangram_serialize(id = 0)]
	pub available: tg::runner::Capacity,

	/// The maximum number of hardware threads that one sandbox can use.
	#[tangram_serialize(id = 1)]
	pub cpu_limit: u64,

	/// The maximum overcommit factor for each resource.
	#[tangram_serialize(id = 2)]
	pub overcommit: tg::runner::Overcommit,

	#[tangram_serialize(id = 3)]
	pub total: tg::runner::Capacity,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Process {
	#[tangram_serialize(id = 0)]
	pub data: tg::process::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::process::Id>,

	#[tangram_serialize(id = 6)]
	pub lease: tg::process::Lease,

	/// The leases that callers acquired from the scheduler while the process was queued.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "Vec::is_empty")]
	pub leases: Vec<tg::process::Lease>,

	/// The stdio opens that the runner services when the process starts, as if each arrived over process control.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Vec::is_empty")]
	pub open: Vec<tg::process::control::ServerRequest>,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "is_default")]
	pub options: tg::referent::Options,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
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
pub struct CreateSandboxServerRequestArg {
	#[tangram_serialize(id = 0)]
	pub arg: tg::sandbox::create::Arg,

	/// The boot to start, which is zero when the request creates the sandbox.
	#[serde(default, skip_serializing_if = "tangram_util::serde::is_default")]
	#[tangram_serialize(
		default,
		id = 9,
		skip_serializing_if = "tangram_util::serde::is_default"
	)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub borrowed: bool,

	#[tangram_serialize(id = 2)]
	pub capacity: tg::runner::Capacity,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "tg::Principal::is_root")]
	pub creator: tg::Principal,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	#[tangram_serialize(id = 8, default, skip_serializing_if = "tg::Principal::is_root")]
	pub owner: tg::Principal,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
	pub process: Option<Process>,

	#[tangram_serialize(id = 6)]
	pub sandbox: tg::sandbox::Id,

	/// Whether a stop of the boot arrived while it was queued, so that the runner stops the boot once it starts, unless a start arrives first.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	#[tangram_serialize(id = 10, default, skip_serializing_if = "std::ops::Not::not")]
	pub stop: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "Option::is_none")]
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
pub struct CreateSandboxClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub created: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DestroySandboxClientRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

	#[tangram_serialize(id = 0)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DestroySandboxServerResponseOutput {
	#[tangram_serialize(id = 0)]
	pub destroyed: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DestroySandboxServerRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 0)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DestroySandboxClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub destroyed: bool,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageClientRequestArg {
	/// The stopped sandboxes that the report covers.
	#[tangram_serialize(id = 0)]
	pub sandboxes: Vec<ReportSandboxUsageClientRequestSandbox>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageClientRequestSandbox {
	/// The boot that the sandbox stopped after.
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,

	/// The storage usage of the stopped directory since the boot stopped, if the runner measured it.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub usage: Option<tg::sandbox::Usage>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageServerResponseOutput {
	/// What the runner does with each sandbox of the report, in the report's order.
	#[tangram_serialize(id = 0)]
	pub sandboxes: Vec<ReportSandboxUsageServerResponseSandbox>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReportSandboxUsageServerResponseSandbox {
	#[tangram_serialize(id = 0)]
	pub action: StoppedSandboxAction,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,
}

/// What a runner does with the directory of a stopped sandbox that it reported.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum StoppedSandboxAction {
	/// Keep the directory.
	#[tangram_serialize(id = 0)]
	Keep,

	/// Remove the directory and report the sandbox's destruction.
	#[tangram_serialize(id = 1)]
	Remove(StoppedSandboxRemoval),
}

/// The reason that a runner removes the directory of a stopped sandbox.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum StoppedSandboxRemoval {
	/// The index records the sandbox as destroyed.
	#[tangram_serialize(id = 0)]
	Destroyed,

	/// The index records the sandbox as destroying, and the directory is all that is left of it.
	#[tangram_serialize(id = 1)]
	Destroying,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct StopSandboxClientRequestArg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

	#[tangram_serialize(id = 0)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct StopSandboxServerResponseOutput {
	#[tangram_serialize(id = 0)]
	pub stopped: bool,
}

#[serde_as]
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
	pub heartbeat: HeartbeatClientNotification,

	#[tangram_serialize(id = 1)]
	pub host: String,

	#[tangram_serialize(id = 2)]
	pub id: tg::runner::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	/// The sandboxes that the runner is running.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Vec::is_empty")]
	pub sandboxes: Vec<ArgSandbox>,

	#[serde_as(as = "DurationSecondsWithFrac")]
	#[tangram_serialize(id = 4)]
	pub scheduler_ttl: Duration,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ArgSandbox {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub id: tg::sandbox::Id,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Header {
	/// How long the scheduler keeps the runner after its last heartbeat, after which the scheduler no longer sends the runner a request again.
	#[serde_as(as = "DurationSecondsWithFrac")]
	#[tangram_serialize(id = 2)]
	pub runner_ttl: Duration,

	/// The sandboxes in the arg that the server stopped, destroyed, or started at another boot while the runner's heartbeat was expired.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub sandboxes: Vec<tg::sandbox::Id>,

	#[tangram_serialize(id = 1)]
	pub scheduler: tg::scheduler::Id,
}

impl tg::Session {
	pub async fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::runner::control::Header,
				impl futures::Stream<Item = tg::Result<tg::runner::control::ServerMessage>>
				+ Send
				+ 'static
				+ use<>,
			),
			tg::Error,
		>,
	> {
		let method = http::Method::POST;
		let uri = Uri::builder().path("/runners/control").build().unwrap();
		let max_frame_size = self.client().sync.max_frame_size;
		let body = crate::frame::encode(stream, max_frame_size);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE)
			.arg_with_tangram(&arg, body)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = match self
			.send_attempt(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?
		{
			ControlFlow::Break(response) => response,
			ControlFlow::Continue(error) => {
				let error = tg::error!(!error, "failed to send the request");
				return Ok(ControlFlow::Continue(error));
			},
		};
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

		// Decode the response, which ends early if the connection is lost.
		let Some((header, stream)) = crate::frame::decode_with_header(
			response.into_body(),
			self.client().http.max_header_length,
			max_frame_size,
		)
		.await?
		else {
			let error = tg::error!("the runner control stream ended before its header");
			return Ok(ControlFlow::Continue(error));
		};

		Ok(ControlFlow::Break((header, stream)))
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

impl TryFrom<ServerMessage> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: ServerMessage) -> Result<Self, Self::Error> {
		let event = match value {
			ServerMessage::Request(request) => {
				let data = serde_json::to_string(&request)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("request".to_owned()),
					..Default::default()
				}
			},
			ServerMessage::Ack(ack) => {
				let data = serde_json::to_string(&ack)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("ack".to_owned()),
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
