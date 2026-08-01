#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Status {
	#[tangram_serialize(id = 1)]
	Incomplete,

	#[tangram_serialize(id = 2)]
	Ready,

	#[default]
	#[tangram_serialize(id = 0)]
	Unconfigured,
}

impl Status {
	#[must_use]
	pub fn from_parts(configured: bool, ready: bool) -> Self {
		if configured && ready {
			Self::Ready
		} else if configured {
			Self::Incomplete
		} else {
			Self::Unconfigured
		}
	}
}
