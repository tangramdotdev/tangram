use {crate::Value, indexmap::IndexMap, tangram_either::Either};

pub use self::{deserialize::Deserialize, serde::Serde, serialize::Serialize};

pub mod deserialize;
pub mod serde;
pub mod serialize;

#[derive(Clone, Debug)]
pub struct Row {
	entries: IndexMap<String, Value>,
}

impl Row {
	pub fn with_entries(entries: impl IntoIterator<Item = (String, Value)>) -> Self {
		let entries = entries.into_iter().collect();
		Self { entries }
	}

	pub fn entries(&self) -> impl Iterator<Item = (&String, &Value)> {
		self.entries.iter()
	}

	pub fn into_entries(self) -> impl Iterator<Item = (String, Value)> {
		self.entries.into_iter()
	}

	pub fn keys(&self) -> impl Iterator<Item = &String> {
		self.entries.keys()
	}

	pub fn into_keys(self) -> impl Iterator<Item = String> {
		self.entries.into_keys()
	}

	pub fn values(&self) -> impl Iterator<Item = &Value> {
		self.entries.values()
	}

	pub fn into_values(self) -> impl Iterator<Item = Value> {
		self.entries.into_values()
	}

	pub fn get<'a>(&self, index: impl Into<Either<usize, &'a str>>) -> Option<&Value> {
		match index.into() {
			Either::Left(index) => self.entries.get_index(index).map(|(_, value)| value),
			Either::Right(index) => self.entries.get(index),
		}
	}
}
