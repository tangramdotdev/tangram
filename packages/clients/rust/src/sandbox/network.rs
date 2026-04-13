use crate as tg;

#[derive(
	Clone,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub enum Network {
	Host,
	Isolated,
}

impl std::fmt::Display for Network {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Host => write!(f, "host"),
			Self::Isolated => write!(f, "isolated"),
		}
	}
}

impl std::str::FromStr for Network {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"host" => Ok(Self::Host),
			"isolated" => Ok(Self::Isolated),
			network => Err(tg::error!(%network, "unknown network option")),
		}
	}
}
