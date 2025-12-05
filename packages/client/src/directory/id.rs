use {crate::prelude::*, std::ops::Deref};

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
#[debug("tg::directory::Id(\"{_0}\")")]
#[serde(into = "crate::Id", try_from = "crate::Id")]
#[tangram_serialize(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(crate::Id);

impl Id {
	#[must_use]
	pub fn new(bytes: &[u8]) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Directory, bytes))
	}
}

impl Deref for Id {
	type Target = crate::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Directory {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}
