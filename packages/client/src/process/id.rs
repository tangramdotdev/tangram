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
#[debug("tg::process::Id(\"{_0}\")")]
#[serde(into = "crate::Id", try_from = "crate::Id")]
#[tangram_serialize(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(crate::Id);

impl Id {
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(crate::Id::new_uuidv7(tg::id::Kind::Process))
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}
}

impl Deref for Id {
	type Target = crate::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Process {
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
