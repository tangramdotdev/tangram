#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Permission {
	#[tangram_serialize(id = 0)]
	Node,

	#[tangram_serialize(id = 1)]
	NodeCommandObjects,

	#[tangram_serialize(id = 2)]
	NodeErrorObjects,

	#[tangram_serialize(id = 3)]
	NodeLogObjects,

	#[tangram_serialize(id = 4)]
	NodeOutputObjects,

	#[tangram_serialize(id = 5)]
	Parent,

	#[tangram_serialize(id = 6)]
	Subtree,

	#[tangram_serialize(id = 7)]
	SubtreeCommandObjects,

	#[tangram_serialize(id = 8)]
	SubtreeErrorObjects,

	#[tangram_serialize(id = 9)]
	SubtreeLogObjects,

	#[tangram_serialize(id = 10)]
	SubtreeOutputObjects,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	Hash,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(from = "Vec<Permission>", into = "Vec<Permission>")]
#[tangram_serialize(into = "Vec<Permission>", try_from = "Vec<Permission>")]
pub struct Set(u16);

impl Set {
	pub const NODE: Self = Self(1 << 0);
	pub const NODE_COMMAND_OBJECTS: Self = Self(1 << 1);
	pub const NODE_ERROR_OBJECTS: Self = Self(1 << 2);
	pub const NODE_LOG_OBJECTS: Self = Self(1 << 3);
	pub const NODE_OUTPUT_OBJECTS: Self = Self(1 << 4);
	pub const PARENT: Self = Self(1 << 10);
	pub const SUBTREE: Self = Self(1 << 5);
	pub const SUBTREE_COMMAND_OBJECTS: Self = Self(1 << 6);
	pub const SUBTREE_ERROR_OBJECTS: Self = Self(1 << 7);
	pub const SUBTREE_LOG_OBJECTS: Self = Self(1 << 8);
	pub const SUBTREE_OUTPUT_OBJECTS: Self = Self(1 << 9);
}

impl Permission {
	#[must_use]
	pub fn to_subtree(self) -> Self {
		match self {
			Self::Node | Self::Subtree => Self::Subtree,
			Self::NodeCommandObjects | Self::SubtreeCommandObjects => Self::SubtreeCommandObjects,
			Self::NodeErrorObjects | Self::SubtreeErrorObjects => Self::SubtreeErrorObjects,
			Self::NodeLogObjects | Self::SubtreeLogObjects => Self::SubtreeLogObjects,
			Self::NodeOutputObjects | Self::SubtreeOutputObjects => Self::SubtreeOutputObjects,
			Self::Parent => Self::Parent,
		}
	}

	#[must_use]
	pub fn implies(self, needed: Self) -> bool {
		self == needed || self == needed.to_subtree() || self == Self::Parent
	}
}

impl Set {
	#[must_use]
	pub fn all() -> Self {
		Self(
			Self::NODE.0
				| Self::NODE_COMMAND_OBJECTS.0
				| Self::NODE_ERROR_OBJECTS.0
				| Self::NODE_LOG_OBJECTS.0
				| Self::NODE_OUTPUT_OBJECTS.0
				| Self::PARENT.0
				| Self::SUBTREE.0
				| Self::SUBTREE_COMMAND_OBJECTS.0
				| Self::SUBTREE_ERROR_OBJECTS.0
				| Self::SUBTREE_LOG_OBJECTS.0
				| Self::SUBTREE_OUTPUT_OBJECTS.0,
		)
	}

	#[must_use]
	pub fn empty() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn from_permission(permission: Permission) -> Self {
		match permission {
			Permission::Node => Self::NODE,
			Permission::NodeCommandObjects => Self::NODE_COMMAND_OBJECTS,
			Permission::NodeErrorObjects => Self::NODE_ERROR_OBJECTS,
			Permission::NodeLogObjects => Self::NODE_LOG_OBJECTS,
			Permission::NodeOutputObjects => Self::NODE_OUTPUT_OBJECTS,
			Permission::Parent => Self::PARENT,
			Permission::Subtree => Self::SUBTREE,
			Permission::SubtreeCommandObjects => Self::SUBTREE_COMMAND_OBJECTS,
			Permission::SubtreeErrorObjects => Self::SUBTREE_ERROR_OBJECTS,
			Permission::SubtreeLogObjects => Self::SUBTREE_LOG_OBJECTS,
			Permission::SubtreeOutputObjects => Self::SUBTREE_OUTPUT_OBJECTS,
		}
	}

	#[must_use]
	pub fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0
	}

	#[must_use]
	pub fn is_empty(self) -> bool {
		self.0 == 0
	}

	pub fn insert(&mut self, other: Self) {
		self.0 |= other.0;
	}

	pub fn iter(self) -> impl Iterator<Item = Permission> {
		[
			self.contains(Self::NODE).then_some(Permission::Node),
			self.contains(Self::NODE_COMMAND_OBJECTS)
				.then_some(Permission::NodeCommandObjects),
			self.contains(Self::NODE_ERROR_OBJECTS)
				.then_some(Permission::NodeErrorObjects),
			self.contains(Self::NODE_LOG_OBJECTS)
				.then_some(Permission::NodeLogObjects),
			self.contains(Self::NODE_OUTPUT_OBJECTS)
				.then_some(Permission::NodeOutputObjects),
			self.contains(Self::PARENT).then_some(Permission::Parent),
			self.contains(Self::SUBTREE).then_some(Permission::Subtree),
			self.contains(Self::SUBTREE_COMMAND_OBJECTS)
				.then_some(Permission::SubtreeCommandObjects),
			self.contains(Self::SUBTREE_ERROR_OBJECTS)
				.then_some(Permission::SubtreeErrorObjects),
			self.contains(Self::SUBTREE_LOG_OBJECTS)
				.then_some(Permission::SubtreeLogObjects),
			self.contains(Self::SUBTREE_OUTPUT_OBJECTS)
				.then_some(Permission::SubtreeOutputObjects),
		]
		.into_iter()
		.flatten()
	}

	pub fn remove(&mut self, other: Self) {
		self.0 &= !other.0;
	}
}

impl From<Vec<Permission>> for Set {
	fn from(permissions: Vec<Permission>) -> Self {
		let mut output = Self::empty();
		for permission in permissions {
			output.insert(Self::from_permission(permission));
		}
		output
	}
}

impl From<Set> for Vec<Permission> {
	fn from(permissions: Set) -> Self {
		permissions.iter().collect()
	}
}
