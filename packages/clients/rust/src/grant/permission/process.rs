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
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Permission {
	Node,
	NodeCommand,
	NodeError,
	NodeLog,
	NodeOutput,
	Subtree,
	SubtreeCommand,
	SubtreeError,
	SubtreeLog,
	SubtreeOutput,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Set(u16);

impl Set {
	pub const NODE: Self = Self(1 << 0);
	pub const NODE_COMMAND: Self = Self(1 << 1);
	pub const NODE_ERROR: Self = Self(1 << 2);
	pub const NODE_LOG: Self = Self(1 << 3);
	pub const NODE_OUTPUT: Self = Self(1 << 4);
	pub const SUBTREE: Self = Self(1 << 5);
	pub const SUBTREE_COMMAND: Self = Self(1 << 6);
	pub const SUBTREE_ERROR: Self = Self(1 << 7);
	pub const SUBTREE_LOG: Self = Self(1 << 8);
	pub const SUBTREE_OUTPUT: Self = Self(1 << 9);
}

impl Permission {
	#[must_use]
	pub fn to_subtree(self) -> Self {
		match self {
			Self::Node | Self::Subtree => Self::Subtree,
			Self::NodeCommand | Self::SubtreeCommand => Self::SubtreeCommand,
			Self::NodeError | Self::SubtreeError => Self::SubtreeError,
			Self::NodeLog | Self::SubtreeLog => Self::SubtreeLog,
			Self::NodeOutput | Self::SubtreeOutput => Self::SubtreeOutput,
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
				| Self::NODE_COMMAND.0
				| Self::NODE_ERROR.0
				| Self::NODE_LOG.0
				| Self::NODE_OUTPUT.0
				| Self::SUBTREE.0
				| Self::SUBTREE_COMMAND.0
				| Self::SUBTREE_ERROR.0
				| Self::SUBTREE_LOG.0
				| Self::SUBTREE_OUTPUT.0,
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
			Permission::NodeCommand => Self::NODE_COMMAND,
			Permission::NodeError => Self::NODE_ERROR,
			Permission::NodeLog => Self::NODE_LOG,
			Permission::NodeOutput => Self::NODE_OUTPUT,
			Permission::Subtree => Self::SUBTREE,
			Permission::SubtreeCommand => Self::SUBTREE_COMMAND,
			Permission::SubtreeError => Self::SUBTREE_ERROR,
			Permission::SubtreeLog => Self::SUBTREE_LOG,
			Permission::SubtreeOutput => Self::SUBTREE_OUTPUT,
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
			self.contains(Self::NODE_COMMAND)
				.then_some(Permission::NodeCommand),
			self.contains(Self::NODE_ERROR)
				.then_some(Permission::NodeError),
			self.contains(Self::NODE_LOG).then_some(Permission::NodeLog),
			self.contains(Self::NODE_OUTPUT)
				.then_some(Permission::NodeOutput),
			self.contains(Self::SUBTREE).then_some(Permission::Subtree),
			self.contains(Self::SUBTREE_COMMAND)
				.then_some(Permission::SubtreeCommand),
			self.contains(Self::SUBTREE_ERROR)
				.then_some(Permission::SubtreeError),
			self.contains(Self::SUBTREE_LOG)
				.then_some(Permission::SubtreeLog),
			self.contains(Self::SUBTREE_OUTPUT)
				.then_some(Permission::SubtreeOutput),
		]
		.into_iter()
		.flatten()
	}

	pub fn remove(&mut self, other: Self) {
		self.0 &= !other.0;
	}
}
