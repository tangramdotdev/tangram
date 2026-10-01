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
	Subtree,
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
pub struct Set(u8);

impl Set {
	pub const NODE: Self = Self(1 << 0);
	pub const SUBTREE: Self = Self(1 << 1);
}

impl Storage {
	/// Raise the storage to its subtree variant.
	#[must_use]
	pub fn subtree(self) -> Self {
		Self::Subtree
	}

	#[must_use]
	pub fn implies(self, needed: Self) -> bool {
		self == needed || self == needed.subtree()
	}
}

impl Set {
	#[must_use]
	pub fn empty() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn from_storage(storage: Storage) -> Self {
		match storage {
			Storage::Node => Self::NODE,
			Storage::Subtree => Self::SUBTREE,
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
			self.contains(Self::SUBTREE).then_some(Storage::Subtree),
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
