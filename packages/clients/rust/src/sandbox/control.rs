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
pub enum ClientNotification {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientRequest {
	pub arg: ClientRequestArg,
	pub id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientRequestArg {
	Destroy(DestroyClientRequestArg),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientResponse {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ClientResponseOutput>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientResponseOutput {
	Destroy(DestroyClientResponseOutput),
	Get(GetClientResponseOutput),
	SpawnProcess(SpawnProcessClientResponseOutput),
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
	Destroy(DestroyServerRequestArg),
	Get(GetServerRequestArg),
	SpawnProcess(SpawnProcessServerRequestArg),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ServerResponse {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ServerResponseOutput>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ServerResponseOutput {
	Destroy(DestroyServerResponseOutput),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DestroyClientRequestArg {
	pub data: tg::sandbox::get::Output,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DestroyServerRequestArg {
	pub error: Option<tg::error::Data>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DestroyClientResponseOutput {
	pub destroyed: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DestroyServerResponseOutput {}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct GetServerRequestArg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetClientResponseOutput {
	pub data: tg::sandbox::get::Output,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SpawnProcessServerRequestArg {
	pub process: tg::runner::control::Process,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SpawnProcessClientResponseOutput {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub grant: Option<tg::grant::Token>,

	pub lease: String,

	pub process: tg::process::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub arg: tg::sandbox::create::Arg,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_at: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub data: Option<Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub runner: Option<tg::runner::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::sandbox::Id,
	pub token: Option<String>,
}

impl tg::Session {
	pub async fn get_sandbox_control_stream(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		impl futures::Stream<Item = tg::Result<tg::sandbox::control::ServerMessage>>
		+ Send
		+ 'static
		+ use<>,
	)> {
		let method = http::Method::POST;
		let path = "/sandboxes/control";
		let uri = Uri::builder()
			.path(path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let stream =
			stream.map(
				|result: tg::Result<tg::sandbox::control::ClientMessage>| match result {
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
		Ok((output, stream))
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
