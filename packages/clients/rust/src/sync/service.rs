use crate::prelude::*;

/// A request to a sync's service to verify a node's permissions and storage.
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
	pub node: tg::Id,

	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::Set,

	#[tangram_serialize(id = 2)]
	pub storage: tg::storage::Set,
}

/// A sync service's response, which is an error, a verified node, or neither when the sync finished without the node.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Response {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub output: Option<Output>,
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
pub enum Output {
	#[tangram_serialize(id = 0)]
	Object(ObjectOutput),

	#[tangram_serialize(id = 1)]
	Process(ProcessOutput),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ObjectOutput {
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
pub struct ProcessOutput {
	#[tangram_serialize(id = 1)]
	pub permissions: tg::authorization::permission::process::Set,

	#[tangram_serialize(id = 0)]
	pub storage: tg::process::storage::Set,

	#[tangram_serialize(id = 2)]
	pub tokens: Vec<tg::authorization::Body>,
}

impl Arg {
	#[must_use]
	pub fn object(
		node: tg::object::Id,
		permissions: tg::authorization::permission::object::Set,
		storage: tg::object::storage::Set,
	) -> Self {
		Self {
			node: node.into(),
			permissions: tg::authorization::permission::Set::Object(permissions),
			storage: tg::storage::Set::Object(storage),
		}
	}

	#[must_use]
	pub fn process(
		node: tg::process::Id,
		permissions: tg::authorization::permission::process::Set,
		storage: tg::process::storage::Set,
	) -> Self {
		Self {
			node: node.into(),
			permissions: tg::authorization::permission::Set::Process(permissions),
			storage: tg::storage::Set::Process(storage),
		}
	}

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

impl Output {
	#[must_use]
	pub fn satisfies(&self, arg: &Arg) -> bool {
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
