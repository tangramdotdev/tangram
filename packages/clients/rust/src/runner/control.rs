use {
	crate::prelude::*,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientMessage {
	Ack(ClientAck),
	Notification(ClientNotification),
	Request(ClientRequest),
	Response(ClientResponse),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerMessage {
	Ack(ServerAck),
	Notification(ServerNotification),
	Request(ServerRequest),
	Response(ServerResponse),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientAck {
	pub id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientNotification {
	Heartbeat(HeartbeatClientNotification),
	SandboxDestroyed(SandboxDestroyedClientNotification),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientRequest {
	pub arg: ClientRequestArg,
	pub id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientRequestArg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientResponse {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ClientResponseOutput>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientResponseOutput {
	CreateSandbox(CreateSandboxClientResponseOutput),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ServerAck {
	pub id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerNotification {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ServerRequest {
	pub arg: ServerRequestArg,
	pub id: String,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerRequestArg {
	CreateSandbox(CreateSandboxServerRequestArg),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ServerResponse {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ServerResponseOutput>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerResponseOutput {}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct HeartbeatClientNotification {
	pub capacity: Capacity,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Capacity {
	pub available: tg::runner::Capacity,
	pub total: tg::runner::Capacity,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ProcessIdentity {
	pub id: tg::process::Id,
	pub token: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Process {
	pub data: tg::process::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub identity: Option<ProcessIdentity>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CreateSandboxServerRequestArg {
	pub arg: tg::sandbox::create::Arg,

	pub capacity: tg::runner::Capacity,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::sandbox::Id>,

	// The process to run in the sandbox once it is started, if any.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process: Option<Process>,

	pub sandbox: tg::sandbox::Id,

	// The token to authenticate as the sandbox.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CreateSandboxClientResponseOutput {
	pub created: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SandboxDestroyedClientNotification {
	pub id: String,

	pub sandbox: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub heartbeat: HeartbeatClientNotification,

	pub host: String,

	pub id: tg::runner::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
}

impl tg::Session {
	pub async fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::runner::control::ServerMessage>>
		+ Send
		+ 'static
		+ use<>,
	> {
		let method = http::Method::POST;
		let path = "/runners/control";
		let uri = Uri::builder()
			.path(path)
			.query_params(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let stream =
			stream.map(
				|result: tg::Result<tg::runner::control::ClientMessage>| match result {
					Ok(message) => message.try_into(),
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
			.map_err(|error| tg::error!(!error, "failed to read a message"))
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
		Ok(stream)
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
			ClientMessage::Response(response) => {
				let data = serde_json::to_string(&response)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("response".to_owned()),
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
			ServerMessage::Notification(notification) => {
				let data = serde_json::to_string(&notification)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				tangram_http::sse::Event {
					data,
					event: Some("notification".to_owned()),
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
