use {super::Stored, tangram_client::prelude::*};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub children: Option<Vec<tg::process::Id>>,
	#[tangram_serialize(id = 1)]
	pub command: tg::object::Id,
	#[tangram_serialize(id = 2)]
	pub data: Option<tg::process::Data>,
	#[tangram_serialize(id = 3)]
	pub error: Field<Vec<tg::object::Id>>,
	#[tangram_serialize(id = 4)]
	pub id: tg::process::Id,
	#[tangram_serialize(id = 5)]
	pub log: Field<tg::object::Id>,
	#[tangram_serialize(id = 6)]
	pub metadata: tg::process::Metadata,
	#[tangram_serialize(id = 7)]
	pub output: Field<Vec<tg::object::Id>>,
	#[tangram_serialize(id = 8)]
	pub parent: Option<tg::process::Id>,
	#[tangram_serialize(id = 9)]
	pub sandbox: Option<tg::sandbox::Id>,
	#[tangram_serialize(id = 10)]
	pub stored: Stored,
	#[tangram_serialize(id = 11)]
	pub time_to_touch: std::time::Duration,
	#[tangram_serialize(id = 12)]
	pub touched_at: i64,
}

/// A field of a put, which the put may leave unset, set to missing, or set to a value. Missing is a distinct variant rather than a nested option because `Option<Option<T>>` cannot round trip: `Some(None)` serializes as null and reads back as `None`.
#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub enum Field<T> {
	#[tangram_serialize(id = 0)]
	Missing,

	#[tangram_serialize(id = 1)]
	Set(T),

	#[tangram_serialize(id = 2)]
	Unset,
}

impl Arg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.set().complete()
			&& self.metadata.subtree.count.is_some()
			&& self.metadata.subtree.depth.is_some()
			&& self.metadata.subtree.command.complete()
			&& self.metadata.subtree.error.complete()
			&& self.metadata.subtree.log.complete()
			&& self.metadata.subtree.output.complete()
			&& self.metadata.node.command.complete()
			&& self.metadata.node.error.complete()
			&& self.metadata.node.log.complete()
			&& self.metadata.node.output.complete()
	}

	#[must_use]
	pub fn set(&self) -> super::Set {
		super::Set {
			children: self.children.is_some(),
			error: !self.error.is_unset(),
			log: !self.log.is_unset(),
			output: !self.output.is_unset(),
		}
	}
}

impl<T> Field<T> {
	#[must_use]
	pub fn is_unset(&self) -> bool {
		matches!(self, Self::Unset)
	}

	#[must_use]
	pub fn value(&self) -> Option<&T> {
		match self {
			Self::Missing | Self::Unset => None,
			Self::Set(value) => Some(value),
		}
	}
}

impl<T> From<Option<T>> for Field<T> {
	fn from(value: Option<T>) -> Self {
		match value {
			None => Self::Missing,
			Some(value) => Self::Set(value),
		}
	}
}
