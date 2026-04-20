use crate::prelude::*;

pub mod arg;

pub use self::arg::Arg;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::IsVariant,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub enum Location {
	Local(Local),
	Remote(Remote),
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct Local {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub region: Option<String>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct Remote {
	#[tangram_serialize(id = 0)]
	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub region: Option<String>,
}

impl std::fmt::Display for Location {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let arg = tg::location::Arg::from(self.clone());
		write!(f, "{arg}")?;
		Ok(())
	}
}

impl std::str::FromStr for Location {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let arg = s.parse::<tg::location::Arg>()?;
		let location = arg
			.to_location()
			.ok_or_else(|| tg::error!("expected exactly one location"))?;
		Ok(location)
	}
}
