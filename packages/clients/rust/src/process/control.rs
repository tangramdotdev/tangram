use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::BytesBase64,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub data: Option<tg::process::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::process::Id>,

	pub lease: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub grant: Option<tg::grant::Token>,

	pub id: tg::process::Id,
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
	#[tangram_serialize(id = 0)]
	pub capacity: tg::runner::Capacity,

	#[tangram_serialize(id = 1)]
	pub parent: tg::sandbox::Id,

	#[tangram_serialize(id = 2)]
	pub runner: tg::runner::Id,
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
	Read(ReadClientResponseOutput),

	#[tangram_serialize(id = 7)]
	ReleaseLease(ReleaseLeaseClientResponseOutput),

	#[tangram_serialize(id = 2)]
	Signal(SignalClientResponseOutput),

	#[tangram_serialize(id = 3)]
	Tty(TtyClientResponseOutput),

	#[tangram_serialize(id = 1)]
	Write(WriteClientResponseOutput),
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
pub enum ServerNotification {}

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
	Read(ReadServerRequestArg),

	#[tangram_serialize(id = 7)]
	ReleaseLease(ReleaseLeaseServerRequestArg),

	#[tangram_serialize(id = 2)]
	Signal(SignalServerRequestArg),

	#[tangram_serialize(id = 3)]
	Tty(TtyServerRequestArg),

	#[tangram_serialize(id = 1)]
	Write(WriteServerRequestArg),
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
pub struct AcquireLeaseClientResponseOutput {
	#[tangram_serialize(id = 0)]
	pub data: tg::process::Data,

	#[tangram_serialize(id = 1)]
	pub lease: Option<String>,
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
	pub lease: String,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReleaseLeaseClientResponseOutput {}

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
pub struct FinishServerRequestArg {
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub exit: u8,
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
pub struct FinishServerResponseOutput {}

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
pub struct GetClientResponseOutput {
	#[tangram_serialize(id = 1)]
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
pub struct ReadServerRequestArg {
	#[tangram_serialize(id = 2)]
	pub length: usize,

	#[tangram_serialize(id = 1)]
	pub stream: tg::process::stdio::Stream,
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
pub struct WriteServerRequestArg {
	#[serde_as(as = "BytesBase64")]
	#[tangram_serialize(id = 2)]
	pub bytes: Bytes,

	#[tangram_serialize(id = 1)]
	pub stream: tg::process::stdio::Stream,
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

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ReadClientResponseOutput {
	#[serde_as(as = "BytesBase64")]
	#[tangram_serialize(id = 2)]
	pub bytes: Bytes,

	#[tangram_serialize(id = 1)]
	pub stream: tg::process::stdio::Stream,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct WriteClientResponseOutput {
	#[tangram_serialize(id = 1)]
	pub length: usize,
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

impl tg::Session {
	pub async fn try_get_process_control_stream(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::control::Output,
			impl futures::Stream<Item = tg::Result<tg::process::control::ServerMessage>>
			+ Send
			+ 'static
			+ use<>,
		)>,
	> {
		let method = http::Method::POST;
		let path = "/processes/control";
		let uri = Uri::builder()
			.path(path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let stream =
			stream.map(
				|result: tg::Result<tg::process::control::ClientMessage>| match result {
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
		let output_in_body = tangram_http::body::output::get_header(response.headers())
			.map_err(|error| tg::error!(!error, "failed to parse the output in body header"))?;
		if !output_in_body {
			return Err(tg::error!("missing the output in body header"));
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
		let mut reader = response.reader();
		let output =
			tangram_http::body::output::get(&mut reader, tangram_http::body::output::MAX_LENGTH)
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the output"))?;
		let stream = tangram_http::sse::decode(reader)
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
		Ok(Some((output, stream)))
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
