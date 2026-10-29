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

	#[tangram_serialize(id = 1)]
	pub borrowed: bool,

	#[tangram_serialize(id = 2)]
	pub capacity: tg::runner::Capacity,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Option::is_none")]
	pub process: Option<Process>,

	#[tangram_serialize(id = 6)]
	pub sandbox: tg::sandbox::Id,

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
pub struct Header {
	#[tangram_serialize(id = 0)]
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
