use {
	super::Stored,
	crate::duration::{deserialize as deserialize_duration, serialize as serialize_duration},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub children: Option<Vec<tg::process::Id>>,
	#[tangram_serialize(id = 1)]
	pub command: tg::object::Id,
	#[tangram_serialize(id = 2)]
	pub data: Option<tg::process::Data>,
	#[tangram_serialize(id = 3)]
	pub error: Option<Option<Vec<tg::object::Id>>>,
	#[tangram_serialize(id = 4)]
	pub id: tg::process::Id,
	#[tangram_serialize(id = 5)]
	pub log: Option<Option<tg::object::Id>>,
	#[tangram_serialize(id = 6)]
	pub metadata: tg::process::Metadata,
	#[tangram_serialize(id = 7)]
	pub output: Option<Option<Vec<tg::object::Id>>>,
	#[tangram_serialize(id = 8)]
	pub parent: Option<tg::process::Id>,
	#[tangram_serialize(id = 9)]
	pub sandbox: Option<tg::sandbox::Id>,
	#[tangram_serialize(id = 10)]
	pub stored: Stored,
	#[tangram_serialize(
		deserialize_with = "deserialize_duration",
		id = 11,
		serialize_with = "serialize_duration"
	)]
	pub time_to_touch: std::time::Duration,
	#[tangram_serialize(id = 12)]
	pub touched_at: i64,
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
			error: self.error.is_some(),
			log: self.log.is_some(),
			output: self.output.is_some(),
		}
	}
}
