use bytes::Bytes;
use crate as tg;

pub mod dequeue;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
)]
pub struct Process {
	id: Id,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind")]
pub enum Event {
	/// The process wrote to stdout
	Stderr(Bytes),
	
	/// The process wrote to stderr. 
	Stdout(Bytes),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub status: tg::build::Status,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub result: Option<tg::Result<tg::value::Data>>,
}

impl From<Id> for tg::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}

impl TryFrom<tg::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Build {
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
