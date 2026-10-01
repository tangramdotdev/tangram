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
pub enum Storage {
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
	Subtree,

	#[tangram_serialize(id = 6)]
	SubtreeCommandObjects,

	#[tangram_serialize(id = 7)]
	SubtreeErrorObjects,

	#[tangram_serialize(id = 8)]
	SubtreeLogObjects,

	#[tangram_serialize(id = 9)]
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
#[serde(from = "Vec<Storage>", into = "Vec<Storage>")]
#[tangram_serialize(into = "Vec<Storage>", try_from = "Vec<Storage>")]
pub struct Set(u16);

impl Set {
	pub const NODE: Self = Self(1 << 0);
	pub const NODE_COMMAND_OBJECTS: Self = Self(1 << 1);
	pub const NODE_ERROR_OBJECTS: Self = Self(1 << 2);
	pub const NODE_LOG_OBJECTS: Self = Self(1 << 3);
	pub const NODE_OUTPUT_OBJECTS: Self = Self(1 << 4);
	pub const SUBTREE: Self = Self(1 << 5);
	pub const SUBTREE_COMMAND_OBJECTS: Self = Self(1 << 6);
	pub const SUBTREE_ERROR_OBJECTS: Self = Self(1 << 7);
	pub const SUBTREE_LOG_OBJECTS: Self = Self(1 << 8);
	pub const SUBTREE_OUTPUT_OBJECTS: Self = Self(1 << 9);
}

impl Storage {
	#[must_use]
	pub fn to_subtree(self) -> Self {
		match self {
			Self::Node | Self::Subtree => Self::Subtree,
			Self::NodeCommandObjects | Self::SubtreeCommandObjects => Self::SubtreeCommandObjects,
			Self::NodeErrorObjects | Self::SubtreeErrorObjects => Self::SubtreeErrorObjects,
			Self::NodeLogObjects | Self::SubtreeLogObjects => Self::SubtreeLogObjects,
			Self::NodeOutputObjects | Self::SubtreeOutputObjects => Self::SubtreeOutputObjects,
		}
	}

	#[must_use]
	pub fn implies(self, needed: Self) -> bool {
		self == needed || self == needed.to_subtree()
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
	pub fn from_storage(storage: Storage) -> Self {
		match storage {
			Storage::Node => Self::NODE,
			Storage::NodeCommandObjects => Self::NODE_COMMAND_OBJECTS,
			Storage::NodeErrorObjects => Self::NODE_ERROR_OBJECTS,
			Storage::NodeLogObjects => Self::NODE_LOG_OBJECTS,
			Storage::NodeOutputObjects => Self::NODE_OUTPUT_OBJECTS,
			Storage::Subtree => Self::SUBTREE,
			Storage::SubtreeCommandObjects => Self::SUBTREE_COMMAND_OBJECTS,
			Storage::SubtreeErrorObjects => Self::SUBTREE_ERROR_OBJECTS,
			Storage::SubtreeLogObjects => Self::SUBTREE_LOG_OBJECTS,
			Storage::SubtreeOutputObjects => Self::SUBTREE_OUTPUT_OBJECTS,
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

	pub fn iter(self) -> impl Iterator<Item = Storage> {
		[
			self.contains(Self::NODE).then_some(Storage::Node),
			self.contains(Self::NODE_COMMAND_OBJECTS)
				.then_some(Storage::NodeCommandObjects),
			self.contains(Self::NODE_ERROR_OBJECTS)
				.then_some(Storage::NodeErrorObjects),
			self.contains(Self::NODE_LOG_OBJECTS)
				.then_some(Storage::NodeLogObjects),
			self.contains(Self::NODE_OUTPUT_OBJECTS)
				.then_some(Storage::NodeOutputObjects),
			self.contains(Self::SUBTREE).then_some(Storage::Subtree),
			self.contains(Self::SUBTREE_COMMAND_OBJECTS)
				.then_some(Storage::SubtreeCommandObjects),
			self.contains(Self::SUBTREE_ERROR_OBJECTS)
				.then_some(Storage::SubtreeErrorObjects),
			self.contains(Self::SUBTREE_LOG_OBJECTS)
				.then_some(Storage::SubtreeLogObjects),
			self.contains(Self::SUBTREE_OUTPUT_OBJECTS)
				.then_some(Storage::SubtreeOutputObjects),
		]
		.into_iter()
		.flatten()
	}

	pub fn remove(&mut self, other: Self) {
		self.0 &= !other.0;
	}
}

impl From<Vec<Storage>> for Set {
	fn from(storages: Vec<Storage>) -> Self {
		let mut output = Self::empty();
		for storage in storages {
			output.insert(Self::from_storage(storage));
		}
		output
	}
}

impl From<Set> for Vec<Storage> {
	fn from(storages: Set) -> Self {
		storages.iter().collect()
	}
}

impl std::ops::BitOr for Set {
	type Output = Self;
	fn bitor(mut self, other: Self) -> Self {
		self.insert(other);
		self
	}
}
