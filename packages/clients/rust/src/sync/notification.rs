use crate::prelude::*;

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Request {
	#[tangram_serialize(id = 0)]
	Object(ObjectRequest),

	#[tangram_serialize(id = 1)]
	Process(ProcessRequest),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ObjectRequest {
	#[tangram_serialize(id = 0)]
	pub id: String,

	#[tangram_serialize(id = 1)]
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
pub struct ProcessRequest {
	#[tangram_serialize(id = 2)]
	pub children: bool,

	#[tangram_serialize(id = 3)]
	pub commands: bool,

	#[tangram_serialize(id = 4)]
	pub errors: bool,

	#[tangram_serialize(id = 0)]
	pub id: String,

	#[tangram_serialize(id = 5)]
	pub logs: bool,

	#[tangram_serialize(id = 1)]
	pub node: tg::process::Id,

	#[tangram_serialize(id = 6)]
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
pub enum Response {
	#[tangram_serialize(id = 0)]
	Object(ObjectResponse),

	#[tangram_serialize(id = 1)]
	Process(ProcessResponse),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ObjectResponse {
	#[tangram_serialize(id = 1)]
	pub available: bool,

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
pub struct ProcessResponse {
	#[tangram_serialize(id = 1)]
	pub available: bool,

	#[tangram_serialize(id = 0)]
	pub id: String,
}

impl Request {
	#[must_use]
	pub fn object(node: tg::object::Id) -> Self {
		let id = uuid::Uuid::now_v7().to_string();
		Self::Object(ObjectRequest { id, node })
	}

	#[must_use]
	pub fn process(
		node: tg::process::Id,
		permissions: tg::authorization::permission::process::Set,
	) -> Self {
		use tg::authorization::permission::process::Set;
		let id = uuid::Uuid::now_v7().to_string();
		let children = permissions.contains(Set::SUBTREE);
		let commands =
			permissions.contains(Set::NODE_COMMAND) || permissions.contains(Set::SUBTREE_COMMAND);
		let errors =
			permissions.contains(Set::NODE_ERROR) || permissions.contains(Set::SUBTREE_ERROR);
		let logs = permissions.contains(Set::NODE_LOG) || permissions.contains(Set::SUBTREE_LOG);
		let outputs =
			permissions.contains(Set::NODE_OUTPUT) || permissions.contains(Set::SUBTREE_OUTPUT);
		Self::Process(ProcessRequest {
			children,
			commands,
			errors,
			id,
			logs,
			node,
			outputs,
		})
	}

	#[must_use]
	pub fn id(&self) -> &str {
		match self {
			Self::Object(request) => &request.id,
			Self::Process(request) => &request.id,
		}
	}

	#[must_use]
	pub fn node(&self) -> tg::Id {
		match self {
			Self::Object(request) => request.node.clone().into(),
			Self::Process(request) => request.node.clone().into(),
		}
	}

	#[must_use]
	pub fn response(&self, available: bool) -> Response {
		match self {
			Self::Object(request) => Response::Object(ObjectResponse {
				available,
				id: request.id.clone(),
			}),
			Self::Process(request) => Response::Process(ProcessResponse {
				available,
				id: request.id.clone(),
			}),
		}
	}
}

impl Response {
	#[must_use]
	pub fn available(&self) -> bool {
		match self {
			Self::Object(response) => response.available,
			Self::Process(response) => response.available,
		}
	}

	#[must_use]
	pub fn id(&self) -> &str {
		match self {
			Self::Object(response) => &response.id,
			Self::Process(response) => &response.id,
		}
	}
}
