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
	#[tangram_serialize(id = 0)]
	Heartbeat(HeartbeatClientRequestArg),

	#[tangram_serialize(id = 1)]
	Verify(VerifyClientRequestArg),
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
	#[tangram_serialize(id = 0)]
	Heartbeat(HeartbeatServerResponseOutput),

	#[tangram_serialize(id = 1)]
	Verify(Option<VerifyServerResponseOutput>),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct VerifyClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub node: tg::Id,

	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::Set,

	#[tangram_serialize(id = 2)]
	pub storage: tg::storage::Set,
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
pub enum VerifyServerResponseOutput {
	#[tangram_serialize(id = 0)]
	Object(VerifyObjectServerResponseOutput),

	#[tangram_serialize(id = 1)]
	Process(VerifyProcessServerResponseOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct VerifyObjectServerResponseOutput {
	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::object::Set,

	#[tangram_serialize(id = 0)]
	pub storage: tg::object::storage::Set,

	#[tangram_serialize(id = 2)]
	pub tokens: Vec<tg::authorization::Body>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct VerifyProcessServerResponseOutput {
	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::process::Set,

	#[tangram_serialize(id = 0)]
	pub storage: tg::process::storage::Set,

	#[tangram_serialize(id = 2)]
	pub tokens: Vec<tg::authorization::Body>,
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
		storage: tg::object::storage::Set,
	) -> Self {
		Self::Verify(VerifyClientRequestArg {
			node: node.into(),
			permissions: tg::authorization::permission::Set::Object(permissions),
			storage: tg::storage::Set::Object(storage),
		})
	}

	#[must_use]
	pub fn process(
		node: tg::process::Id,
		permissions: tg::authorization::permission::process::Set,
		storage: tg::process::storage::Set,
	) -> Self {
		Self::Verify(VerifyClientRequestArg {
			node: node.into(),
			permissions: tg::authorization::permission::Set::Process(permissions),
			storage: tg::storage::Set::Process(storage),
		})
	}

	#[must_use]
	pub fn node(&self) -> Option<tg::Id> {
		match self {
			Self::Verify(arg) => Some(arg.node.clone()),
			Self::Heartbeat(_) => None,
		}
	}
}

impl VerifyClientRequestArg {
	pub fn validate(&self) -> tg::Result<()> {
		let valid = match (&self.permissions, &self.storage) {
			(tg::authorization::permission::Set::Object(_), tg::storage::Set::Object(_)) => {
				self.node.kind().is_object()
			},
			(tg::authorization::permission::Set::Process(_), tg::storage::Set::Process(_)) => {
				self.node.kind() == tg::id::Kind::Process
			},
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

impl VerifyServerResponseOutput {
	#[must_use]
	pub fn satisfies(&self, arg: &VerifyClientRequestArg) -> bool {
		if !self.permissions().contains(arg.permissions) {
			return false;
		}
		match (self, arg.storage) {
			(Self::Object(output), tg::storage::Set::Object(required)) => {
				output.storage.contains(required)
			},
			(Self::Process(output), tg::storage::Set::Process(required)) => {
				output.storage.contains(required)
			},
			_ => false,
		}
	}

	#[must_use]
	pub fn tokens(&self) -> &[tg::authorization::Body] {
		match self {
			Self::Object(output) => &output.tokens,
			Self::Process(output) => &output.tokens,
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
