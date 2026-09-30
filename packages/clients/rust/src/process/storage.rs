use tangram_util::serde::is_false;

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
pub struct Storage {
	/// Whether this node's command object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub node_command_objects: bool,

	/// Whether this node's error object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "is_false")]
	pub node_error_objects: bool,

	/// Whether this node's log object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_false")]
	pub node_log_objects: bool,

	/// Whether this node's output object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_false")]
	pub node_output_objects: bool,

	/// Whether this node's subtree is stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_false")]
	pub subtree: bool,

	/// Whether this node's subtree's command object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "is_false")]
	pub subtree_command_objects: bool,

	/// Whether this node's subtree's error object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "is_false")]
	pub subtree_error_objects: bool,

	/// Whether this node's subtree's log object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "is_false")]
	pub subtree_log_objects: bool,

	/// Whether this node's subtree's output object subtrees are stored.
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "is_false")]
	pub subtree_output_objects: bool,
}

impl Storage {
	#[must_use]
	pub fn contains(&self, other: &Self) -> bool {
		(!other.node_command_objects || self.node_command_objects)
			&& (!other.node_error_objects || self.node_error_objects)
			&& (!other.node_log_objects || self.node_log_objects)
			&& (!other.node_output_objects || self.node_output_objects)
			&& (!other.subtree || self.subtree)
			&& (!other.subtree_command_objects || self.subtree_command_objects)
			&& (!other.subtree_error_objects || self.subtree_error_objects)
			&& (!other.subtree_log_objects || self.subtree_log_objects)
			&& (!other.subtree_output_objects || self.subtree_output_objects)
	}

	pub fn merge(&mut self, other: &Self) {
		self.node_command_objects = self.node_command_objects || other.node_command_objects;
		self.node_error_objects = self.node_error_objects || other.node_error_objects;
		self.node_log_objects = self.node_log_objects || other.node_log_objects;
		self.node_output_objects = self.node_output_objects || other.node_output_objects;
		self.subtree = self.subtree || other.subtree;
		self.subtree_command_objects =
			self.subtree_command_objects || other.subtree_command_objects;
		self.subtree_error_objects = self.subtree_error_objects || other.subtree_error_objects;
		self.subtree_log_objects = self.subtree_log_objects || other.subtree_log_objects;
		self.subtree_output_objects = self.subtree_output_objects || other.subtree_output_objects;
	}
}
