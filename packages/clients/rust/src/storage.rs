use crate::prelude::*;

#[cfg(test)]
mod tests;

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Set {
	#[tangram_serialize(id = 0)]
	Object(tg::object::storage::Set),

	#[tangram_serialize(id = 1)]
	Process(tg::process::storage::Set),
}

impl Set {
	#[must_use]
	pub fn empty_like(self) -> Self {
		match self {
			Self::Object(_) => Self::Object(tg::object::storage::Set::empty()),
			Self::Process(_) => Self::Process(tg::process::storage::Set::empty()),
		}
	}
	#[must_use]
	pub fn is_empty(self) -> bool {
		match self {
			Self::Object(storage) => storage.is_empty(),
			Self::Process(storage) => storage.is_empty(),
		}
	}
	#[must_use]
	pub fn contains(self, other: Self) -> bool {
		match (self, other) {
			(Self::Object(storage), Self::Object(other)) => storage.contains(other),
			(Self::Process(storage), Self::Process(other)) => storage.contains(other),
			_ => false,
		}
	}
	pub fn insert(&mut self, other: Self) {
		match (self, other) {
			(Self::Object(storage), Self::Object(other)) => storage.insert(other),
			(Self::Process(storage), Self::Process(other)) => storage.insert(other),
			_ => {},
		}
	}
}
