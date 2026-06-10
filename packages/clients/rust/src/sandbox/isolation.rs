use crate::prelude::*;

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::Display,
	derive_more::IsVariant,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Isolation {
	Container,
	Seatbelt,
	Vm,
}

impl std::str::FromStr for Isolation {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		match value {
			"container" => Ok(Self::Container),
			"seatbelt" => Ok(Self::Seatbelt),
			"vm" => Ok(Self::Vm),
			_ => Err(tg::error!(%value, "invalid isolation")),
		}
	}
}
