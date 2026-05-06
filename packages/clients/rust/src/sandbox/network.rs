use {crate::prelude::*, std::str::FromStr};

#[derive(
	Clone,
	Debug,
	Default,
	derive_more::IsVariant,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Network {
	#[default]
	Host,
	Bridge,
}

impl FromStr for Network {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"host" => Ok(Self::Host),
			"bridge" => Ok(Self::Bridge),
			option => Err(tg::error!(%option, "unknown network option")),
		}
	}
}
