use {
	tangram_client::prelude::*,
	tangram_util::serde::{is_default, is_false},
};

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
	pub stored: Stored,

	#[tangram_serialize(id = 3)]
	pub touched_at: i64,
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
	/// Whether this node's children are set.
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

/// The stored status of a process in the index.
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
pub struct Stored {
	/// Whether this node's command's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub node_command: bool,

	/// Whether this node's error's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "is_false")]
	pub node_error: bool,

	/// Whether this node's log's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub node_log: bool,

	/// Whether this node's outputs' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_false")]
	pub node_output: bool,

	/// Whether this node's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_false")]
	pub subtree: bool,

	/// Whether this node's subtree's commands' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_false")]
	pub subtree_command: bool,

	/// Whether this node's subtree's errors' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "is_false")]
	pub subtree_error: bool,

	/// Whether this node's subtree's logs' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "is_false")]
	pub subtree_log: bool,

	/// Whether this node's subtree's outputs' subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "is_false")]
	pub subtree_output: bool,
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

impl Stored {
	pub fn merge(&mut self, other: &Self) {
		self.node_command = self.node_command || other.node_command;
		self.node_error = self.node_error || other.node_error;
		self.node_log = self.node_log || other.node_log;
		self.node_output = self.node_output || other.node_output;
		self.subtree = self.subtree || other.subtree;
		self.subtree_command = self.subtree_command || other.subtree_command;
		self.subtree_error = self.subtree_error || other.subtree_error;
		self.subtree_log = self.subtree_log || other.subtree_log;
		self.subtree_output = self.subtree_output || other.subtree_output;
	}
}
