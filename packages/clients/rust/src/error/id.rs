use crate::prelude::*;

#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Debug,
	derive_more::Display,
	derive_more::Into,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[debug("tg::error::Id(\"{_0}\")")]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

impl tg::error::Id {
	#[must_use]
	pub fn new(bytes: &[u8]) -> Self {
		Self(tg::Id::new_blake3(tg::id::Kind::Error, bytes))
	}
}

impl std::ops::Deref for tg::error::Id {
	type Target = tg::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl TryFrom<tg::Id> for tg::error::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Error {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for tg::error::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for tg::error::Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}
