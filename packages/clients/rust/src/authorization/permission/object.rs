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
#[serde(from = "Vec<Permission>", into = "Vec<Permission>")]
#[tangram_serialize(into = "Vec<Permission>", try_from = "Vec<Permission>")]
pub struct Set(u8);

impl Set {
	pub const NODE: Self = Self(1 << 0);
	pub const SUBTREE: Self = Self(1 << 1);
}

impl Permission {
	/// Raise the permission to its subtree variant.
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
	pub fn from_permission(permission: Permission) -> Self {
		match permission {
			Permission::Node => Self::NODE,
			Permission::Subtree => Self::SUBTREE,
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
			self.contains(Self::SUBTREE).then_some(Permission::Subtree),
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
