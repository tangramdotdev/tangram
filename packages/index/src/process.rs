use {
	tangram_client::prelude::*,
	tangram_util::serde::{is_default, is_false},
};

mod storage;

pub use storage::Storage;

pub mod object;
pub mod put;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Process {
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub data: Option<tg::process::Data>,

	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub metadata: tg::process::Metadata,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<tg::sandbox::Id>,

	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_default")]
	pub set: Set,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub storage: Storage,

	#[tangram_serialize(id = 3)]
	pub touched_at: i64,
}

#[derive(Clone, Debug)]
pub struct NodeChildren {
	pub complete: bool,
	pub nodes: Vec<tg::Referent<tg::Id>>,
}

/// The set status of a process in the index.
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Set {
	/// Whether the complete children list for this node is set.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub children: bool,

	/// Whether this node's error is set.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub error: bool,

	/// Whether this node's log is set.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_false")]
	pub log: bool,

	/// Whether this node's output is set.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_false")]
	pub output: bool,
}

impl Process {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the process"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the process"))
	}
}

impl Set {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.children && self.error && self.log && self.output
	}

	pub fn merge(&mut self, other: &Self) {
		self.children = self.children || other.children;
		self.error = self.error || other.error;
		self.log = self.log || other.log;
		self.output = self.output || other.output;
	}
}
