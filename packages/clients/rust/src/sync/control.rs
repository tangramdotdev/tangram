use {crate::prelude::*, std::time::Duration};

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

	#[tangram_serialize(id = 2)]
	Cancel(ClientCancel),

	#[tangram_serialize(id = 1)]
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
	Ack(ServerAck),

	#[tangram_serialize(id = 1)]
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
	#[tangram_serialize(id = 1)]
	pub attempt: String,

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
pub struct ClientCancel {
	#[tangram_serialize(id = 1)]
	pub attempt: String,

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
pub struct ClientRequest {
	#[tangram_serialize(id = 0)]
	pub arg: ClientRequestArg,

	#[tangram_serialize(id = 3)]
	/// Absent for heartbeats, which start an attempt or keep the current attempt alive.
	pub attempt: Option<String>,

	#[tangram_serialize(id = 1)]
	/// Identifies the requesting sync and its stable reply subject.
	pub client: String,

	#[tangram_serialize(id = 2)]
	/// Remains the same when the request is registered under a replacement attempt.
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
	#[tangram_serialize(id = 1)]
	Get(GetClientRequestArg),

	#[tangram_serialize(id = 0)]
	Heartbeat(HeartbeatClientRequestArg),
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
	#[tangram_serialize(id = 1)]
	pub attempt: String,

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
	#[tangram_serialize(id = 2)]
	pub attempt: String,

	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub id: String,

	#[tangram_serialize(id = 3)]
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
	#[tangram_serialize(id = 1)]
	Get(Option<GetServerResponseOutput>),

	#[tangram_serialize(id = 0)]
	Heartbeat(HeartbeatServerResponseOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub node: tg::Id,

	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::Set,

	#[tangram_serialize(id = 2)]
	pub storage: Option<tg::Storage>,
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
pub enum GetServerResponseOutput {
	#[tangram_serialize(id = 0)]
	Object(GetObjectServerResponseOutput),

	#[tangram_serialize(id = 1)]
	Process(GetProcessServerResponseOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetObjectServerResponseOutput {
	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::object::Set,

	#[tangram_serialize(id = 0)]
	pub storage: Option<tg::object::Storage>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct GetProcessServerResponseOutput {
	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::process::Set,

	#[tangram_serialize(id = 0)]
	pub storage: Option<tg::process::Storage>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct HeartbeatClientRequestArg {}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct HeartbeatServerResponseOutput {
	#[tangram_serialize(id = 0)]
	pub ttl: Duration,
}

impl ClientRequestArg {
	#[must_use]
	pub fn object(
		node: tg::object::Id,
		permissions: tg::authorization::permission::object::Set,
		storage: Option<tg::object::Storage>,
	) -> Self {
		Self::Get(GetClientRequestArg {
			node: node.into(),
			permissions: tg::authorization::permission::Set::Object(permissions),
			storage: storage.map(tg::Storage::Object),
		})
	}

	#[must_use]
	pub fn process(
		node: tg::process::Id,
		permissions: tg::authorization::permission::process::Set,
		storage: Option<tg::process::Storage>,
	) -> Self {
		Self::Get(GetClientRequestArg {
			node: node.into(),
			permissions: tg::authorization::permission::Set::Process(permissions),
			storage: storage.map(tg::Storage::Process),
		})
	}

	#[must_use]
	pub fn node(&self) -> Option<tg::Id> {
		match self {
			Self::Get(arg) => Some(arg.node.clone()),
			Self::Heartbeat(_) => None,
		}
	}
}

impl GetClientRequestArg {
	pub fn validate(&self) -> tg::Result<()> {
		let valid = match (&self.permissions, &self.storage) {
			(
				tg::authorization::permission::Set::Object(_),
				None | Some(tg::Storage::Object(_)),
			) => self.node.kind().is_object(),
			(
				tg::authorization::permission::Set::Process(_),
				None | Some(tg::Storage::Process(_)),
			) => self.node.kind() == tg::id::Kind::Process,
			_ => false,
		};
		if !valid {
			return Err(tg::error!(
				"the sync request permissions or storage do not match the node"
			));
		}
		Ok(())
	}
}

impl GetServerResponseOutput {
	#[must_use]
	pub fn satisfies(&self, arg: &GetClientRequestArg) -> bool {
		if !self.permissions().contains(arg.permissions) {
			return false;
		}
		match (self, &arg.storage) {
			(_, None) => true,
			(Self::Object(output), Some(tg::Storage::Object(required))) => output
				.storage
				.as_ref()
				.is_some_and(|storage| storage.contains(required)),
			(Self::Process(output), Some(tg::Storage::Process(required))) => output
				.storage
				.as_ref()
				.is_some_and(|storage| storage.contains(required)),
			_ => false,
		}
	}

	#[must_use]
	pub fn permissions(&self) -> tg::authorization::permission::Set {
		match self {
			Self::Object(output) => tg::authorization::permission::Set::Object(output.permissions),
			Self::Process(output) => {
				tg::authorization::permission::Set::Process(output.permissions)
			},
		}
	}
}
