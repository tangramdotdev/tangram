use {
	super::Stored,
	crate::duration::{deserialize as deserialize_duration, serialize as serialize_duration},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub cache_entry: Option<tg::artifact::Id>,
	#[tangram_serialize(id = 1)]
	pub children: BTreeSet<tg::object::Id>,
	#[tangram_serialize(id = 2)]
	pub id: tg::object::Id,
	#[tangram_serialize(id = 3)]
	pub metadata: tg::object::Metadata,
	#[tangram_serialize(id = 4)]
	pub stored: Stored,
	#[tangram_serialize(
		deserialize_with = "deserialize_duration",
		id = 5,
		serialize_with = "serialize_duration"
	)]
	pub time_to_touch: std::time::Duration,
	#[tangram_serialize(id = 6)]
	pub touched_at: i64,
}

impl Arg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.metadata.subtree.complete()
	}
}
