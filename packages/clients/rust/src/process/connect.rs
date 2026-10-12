use {
	crate::prelude::*,
	futures::{StreamExt as _, stream::BoxStream},
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::is_false,
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

	/// Whether an upstream routing hop pushes the command objects, using the sync in the destination's header.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "is_false")]
	pub push: bool,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 5,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,
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

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Header {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
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
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum ClientNotification {
	#[tangram_serialize(id = 1)]
	Ready,
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

	#[tangram_serialize(id = 3)]
	Detach,

	#[tangram_serialize(id = 5)]
	Signal(tg::process::signal::post::Arg),

	#[tangram_serialize(id = 6)]
	Tty(tg::process::tty::size::put::Arg),
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
	#[tangram_serialize(id = 2)]
	Outcome(tg::process::outcome::Data),

	#[tangram_serialize(id = 0)]
	Progress(tg::progress::Event<tg::process::spawn::Output>),
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

	#[tangram_serialize(id = 3)]
	Detach,

	#[tangram_serialize(id = 5)]
	Signal,

	#[tangram_serialize(id = 6)]
	Tty,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub lease: Option<String>,
	pub location: Option<tg::location::Arg>,
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
	pub event_capacity: usize,
	pub input_capacity: usize,
	pub max_messages: usize,
	pub max_requests: usize,
	pub message_capacity: usize,
}

pub async fn connect(id: tg::process::Id, options: Options) -> tg::Result<tg::Process> {
	tg::Process::connect(id, options).await
}

pub async fn connect_with_instance<I: tg::Instance>(
	instance: &I,
	id: tg::process::Id,
	options: Options,
) -> tg::Result<tg::Process> {
	tg::Process::connect_with_instance(instance, id, options).await
}

impl<O: 'static> tg::Process<O> {
	pub(super) async fn connect_spawn_with_progress_with_instance<I, F, Fut>(
		instance: &I,
		arg: tg::process::Arg,
		mode: Mode,
		progress: F,
	) -> tg::Result<Self>
	where
		I: tg::Instance,
		F: FnOnce(
			BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		) -> Fut,
		Fut: Future<Output = tg::Result<tg::process::spawn::Output>>,
	{
		let arg = super::spawn::spawn_arg_with_instance(instance, arg).await?;
		let options = tg::process::spawn::Options { mode };
		Self::spawn_inner_with_instance(instance, arg, options, progress).await
	}

	pub async fn connect(id: tg::process::Id, options: Options) -> tg::Result<Self> {
		let instance = tg::instance()?;
		Self::connect_with_instance(instance, id, options).await
	}

	pub async fn connect_with_instance<I: tg::Instance>(
		instance: &I,
		id: tg::process::Id,
		options: Options,
	) -> tg::Result<Self> {
		let arg = Arg {
			lease: options.lease,
			location: options.location,
			mode: Mode::Run,
			process: tg::Either::Right(id.clone()),
			push: false,
			tokens: options.tokens,
		};
		let (connection, progress) = Connection::open(instance, arg).await?;
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
		let instance = tg::instance::dynamic::Instance::new(instance.clone());
		let process = Self::new_inner(id, options, Some(instance), Some(connection));
		Ok(process)
	}
}

impl tg::Session {
	pub async fn try_get_process_connect_stream(
		&self,
		arg: Arg,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<Option<(Header, BoxStream<'static, tg::Result<ServerMessage>>)>> {
		let max_frame_size = self.client().sync.max_frame_size;
		let body = super::stdio::encode(input, max_frame_size);
		let request = http::Request::builder()
			.method(http::Method::POST)
			.uri("/processes/connect")
			.header(http::header::ACCEPT, TANGRAM_CONTENT_TYPE)
			.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE);
		// Preserve native spawn values that cannot round-trip through a query.
		let request = if arg.process.is_left() {
			request.header(tangram_http::body::arg::HEADER, "true")
		} else {
			request
		};
		let request = request
			.arg_with_tangram(&arg, body)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
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
		let (header, stream) = super::stdio::decode_with_header(
			response.into_body(),
			tangram_http::body::encoding::Encoding::Tangram,
			self.client().http.max_header_length,
			max_frame_size,
		)
		.await?;
		Ok(Some((header, stream)))
	}
}

impl Config {
	pub(crate) fn validate(self) -> tg::Result<Self> {
		if self.event_capacity == 0 {
			return Err(tg::error!(
				"expected the event capacity to be greater than zero"
			));
		}
		if self.input_capacity == 0 {
			return Err(tg::error!(
				"expected the input capacity to be greater than zero"
			));
		}
		if self.max_messages == 0 {
			return Err(tg::error!(
				"expected the maximum number of messages to be greater than zero"
			));
		}
		if self.max_requests == 0 {
			return Err(tg::error!(
				"expected the maximum number of requests to be greater than zero"
			));
		}
		if self.message_capacity == 0 {
			return Err(tg::error!(
				"expected the message capacity to be greater than zero"
			));
		}
		Ok(self)
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			event_capacity: 64,
			input_capacity: 64,
			max_messages: 128,
			max_requests: 64,
			message_capacity: 64,
		}
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
