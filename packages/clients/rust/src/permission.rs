use crate::prelude::*;

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
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Permission {
	Read,
	Write,
}

impl tg::Permission {
	#[must_use]
	pub fn implies(self, permission: Self) -> bool {
		match (self, permission) {
			(Self::Write, Self::Read | Self::Write) | (Self::Read, Self::Read) => true,
			(Self::Read, Self::Write) => false,
		}
	}
}
