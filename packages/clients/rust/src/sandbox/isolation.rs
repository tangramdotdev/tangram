use crate::prelude::*;

#[derive(
	Clone, Copy, Debug, derive_more::IsVariant, Eq, PartialEq, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Isolation {
	Container,
	Seatbelt,
	Vm,
}

impl std::str::FromStr for Isolation {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"container" => Ok(Self::Container),
			"seatbelt" => Ok(Self::Seatbelt),
			"vm" => Ok(Self::Vm),
			option => Err(tg::error!(%option, "unknown isolation option")),
		}
	}
}
