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
	#[tangram_serialize(id = 0)]
	pub id: String,

	#[tangram_serialize(id = 1)]
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
pub struct ClientCancel {
	#[tangram_serialize(id = 0)]
	pub id: String,

	#[tangram_serialize(id = 1)]
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
pub struct ClientRequest {
	#[tangram_serialize(id = 0)]
	pub arg: ClientRequestArg,

	#[tangram_serialize(id = 1)]
	/// Identifies the requesting sync and its stable reply subject.
	pub client: String,

	#[tangram_serialize(id = 2)]
	/// Remains the same when the request is registered under a replacement lease.
	pub id: String,

	#[tangram_serialize(id = 3)]
	/// Absent for heartbeats, which create or renew the requesting sync's lease.
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
	#[tangram_serialize(id = 0)]
	pub id: String,

	#[tangram_serialize(id = 1)]
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
pub struct ServerResponse {
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub id: String,

	#[tangram_serialize(id = 2)]
	pub lease: String,

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
	Get(GetServerResponseOutput),

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
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum GetClientRequestArg {
	#[tangram_serialize(id = 0)]
	Object(GetObjectClientRequestArg),

	#[tangram_serialize(id = 1)]
	Process(GetProcessClientRequestArg),
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
pub struct GetObjectClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub node: tg::object::Id,
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
pub struct GetProcessClientRequestArg {
	#[tangram_serialize(id = 0)]
	pub children: bool,

	#[tangram_serialize(id = 1)]
	pub commands: bool,

	#[tangram_serialize(id = 2)]
	pub errors: bool,

	#[tangram_serialize(id = 3)]
	pub logs: bool,

	#[tangram_serialize(id = 4)]
	pub node: tg::process::Id,

	#[tangram_serialize(id = 5)]
	pub outputs: bool,
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
	pub fn object(node: tg::object::Id) -> Self {
		Self::Get(GetClientRequestArg::Object(GetObjectClientRequestArg {
			node,
		}))
	}

	#[must_use]
	pub fn process(
		node: tg::process::Id,
		permissions: tg::authorization::permission::process::Set,
	) -> Self {
		use tg::authorization::permission::process::Set;
		let children = permissions.contains(Set::SUBTREE);
		let commands =
			permissions.contains(Set::NODE_COMMAND) || permissions.contains(Set::SUBTREE_COMMAND);
		let errors =
			permissions.contains(Set::NODE_ERROR) || permissions.contains(Set::SUBTREE_ERROR);
		let logs = permissions.contains(Set::NODE_LOG) || permissions.contains(Set::SUBTREE_LOG);
		let outputs =
			permissions.contains(Set::NODE_OUTPUT) || permissions.contains(Set::SUBTREE_OUTPUT);
		Self::Get(GetClientRequestArg::Process(GetProcessClientRequestArg {
			children,
			commands,
			errors,
			logs,
			node,
			outputs,
		}))
	}

	#[must_use]
	pub fn node(&self) -> Option<tg::Id> {
		match self {
			Self::Get(GetClientRequestArg::Object(arg)) => Some(arg.node.clone().into()),
			Self::Get(GetClientRequestArg::Process(arg)) => Some(arg.node.clone().into()),
			Self::Heartbeat(_) => None,
		}
	}
}

impl GetServerResponseOutput {
	#[must_use]
	pub fn is_stored(&self) -> bool {
		match self {
			Self::Object(output) => output.storage.is_some(),
			Self::Process(output) => output.storage.is_some(),
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
