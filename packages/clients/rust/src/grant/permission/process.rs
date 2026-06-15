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
