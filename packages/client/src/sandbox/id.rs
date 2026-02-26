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
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[debug("tg::sandbox::Id(\"{_0}\")")]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

impl tg::sandbox::Id {
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(tg::Id::new_uuidv7(tg::id::Kind::Sandbox))
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}
}

impl std::ops::Deref for tg::sandbox::Id {
	type Target = tg::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<tg::sandbox::Id> for tg::Id {
	fn from(value: tg::sandbox::Id) -> Self {
		value.0
	}
}

impl TryFrom<tg::Id> for tg::sandbox::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Sandbox {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl TryFrom<Vec<u8>> for tg::sandbox::Id {
	type Error = tg::Error;

	fn try_from(value: Vec<u8>) -> tg::Result<Self> {
		Self::from_slice(&value)
	}
}

impl std::str::FromStr for tg::sandbox::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for tg::sandbox::Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}
