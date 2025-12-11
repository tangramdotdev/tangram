use {crate as tg, std::ops::Deref};

#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Debug,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[debug("tg::graph::Id(\"{_0}\")")]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::object::Id);

impl Id {
	#[must_use]
	pub fn new(bytes: &[u8]) -> Self {
		let id = tg::Id::new_blake3(tg::id::Kind::Graph, bytes);
		Self(tg::object::Id::try_from(id).unwrap())
	}
}

impl Deref for Id {
	type Target = tg::object::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl TryFrom<tg::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Graph {
			return Err(tg::error!(%value, "expected a graph ID"));
		}
		Ok(Self(tg::object::Id::try_from(value)?))
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}

impl From<Id> for tg::Id {
	fn from(value: Id) -> Self {
		value.0.into()
	}
}

impl From<Id> for tg::object::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}
