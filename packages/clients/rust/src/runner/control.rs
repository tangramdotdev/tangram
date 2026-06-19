use {
	crate::prelude::*,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum InputEvent {
	Heartbeat(Heartbeat),
	SandboxCreated(SandboxCreated),
	SandboxDestroyed(tg::sandbox::Id),
	ProcessSpawned(ProcessSpawned),
	End,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum OutputEvent {
	CreateSandbox(CreateSandbox),
	CreateSandboxAck(String),
	SpawnProcess(SpawnProcess),
	SpawnProcessAck(String),
	End,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Heartbeat {
	pub cpus: Capacity,
	pub memory: Capacity,
	pub permits: Capacity,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Capacity {
	pub used: u64,
	pub total: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CreateSandbox {
	pub request_id: String,
	pub id: tg::sandbox::Id,
	pub arg: tg::sandbox::create::Arg,

	// The process to run in the sandbox once it is created, if any.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process: Option<tg::process::Id>,

	// The token to authenticate as the sandbox.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	// The token to authenticate as the process.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_token: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SandboxCreated {
	pub request_id: String,
	pub id: tg::sandbox::Id,
	pub created: bool,

	// Whether the runner created the sandbox itself via the local permit fast path.
	#[serde(default)]
	pub runner: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SpawnProcess {
	pub request_id: String,
	pub sandbox: tg::sandbox::Id,
	pub id: tg::process::Id,

	// The token to authenticate as the process.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub process_token: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ProcessSpawned {
	pub request_id: String,
	pub id: tg::process::Id,
	pub spawned: bool,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub host: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
}

impl tg::Session {
	pub async fn get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::InputEvent>>,
	) -> tg::Result<
		impl futures::Stream<Item = tg::Result<tg::runner::control::OutputEvent>>
		+ Send
		+ 'static
		+ use<>,
	> {
		let method = http::Method::POST;
		let path = format!("/runner/{id}/control");
		let uri = Uri::builder()
			.path(&path)
			.query_params(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let stream = stream.map(
			|result: tg::Result<tg::runner::control::InputEvent>| match result {
				Ok(event) => event.try_into(),
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
			.map_err(|error| tg::error!(!error, "failed to read an event"))
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

impl TryFrom<InputEvent> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: InputEvent) -> Result<Self, Self::Error> {
		let event = match value {
			InputEvent::Heartbeat(heartbeat) => {
				let data = serde_json::to_string(&heartbeat)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("heartbeat".to_owned()),
					..Default::default()
				}
			},
			InputEvent::SandboxCreated(sandbox_created) => {
				let data = serde_json::to_string(&sandbox_created)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("sandbox_created".to_owned()),
					..Default::default()
				}
			},
			InputEvent::SandboxDestroyed(id) => {
				let data = serde_json::to_string(&id)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("sandbox_destroyed".to_owned()),
					..Default::default()
				}
			},
			InputEvent::ProcessSpawned(process_spawned) => {
				let data = serde_json::to_string(&process_spawned)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("process_spawned".to_owned()),
					..Default::default()
				}
			},
			InputEvent::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for InputEvent {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("heartbeat") => {
				let heartbeat = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Heartbeat(heartbeat))
			},
			Some("sandbox_created") => {
				let sandbox_created = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::SandboxCreated(sandbox_created))
			},
			Some("sandbox_destroyed") => {
				let id = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::SandboxDestroyed(id))
			},
			Some("process_spawned") => {
				let process_spawned = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::ProcessSpawned(process_spawned))
			},
			Some("end") => Ok(Self::End),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}

impl TryFrom<OutputEvent> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: OutputEvent) -> Result<Self, Self::Error> {
		let event = match value {
			OutputEvent::CreateSandbox(create_sandbox) => {
				let data = serde_json::to_string(&create_sandbox)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("create_sandbox".to_owned()),
					..Default::default()
				}
			},
			OutputEvent::CreateSandboxAck(request_id) => {
				let data = serde_json::to_string(&request_id)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("create_sandbox_ack".to_owned()),
					..Default::default()
				}
			},
			OutputEvent::SpawnProcess(spawn_process) => {
				let data = serde_json::to_string(&spawn_process)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("spawn_process".to_owned()),
					..Default::default()
				}
			},
			OutputEvent::SpawnProcessAck(request_id) => {
				let data = serde_json::to_string(&request_id)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("spawn_process_ack".to_owned()),
					..Default::default()
				}
			},
			OutputEvent::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for OutputEvent {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("create_sandbox") => {
				let create_sandbox = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::CreateSandbox(create_sandbox))
			},
			Some("create_sandbox_ack") => {
				let request_id = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::CreateSandboxAck(request_id))
			},
			Some("spawn_process") => {
				let spawn_process = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::SpawnProcess(spawn_process))
			},
			Some("spawn_process_ack") => {
				let request_id = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::SpawnProcessAck(request_id))
			},
			Some("end") => Ok(Self::End),
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Err(error)
			},
			_ => Err(tg::error!("invalid event")),
		}
	}
}
