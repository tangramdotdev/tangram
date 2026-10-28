use {super::storage::Set, std::collections::BTreeSet, tangram_client::prelude::*};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	/// The account for which to put a storage entry for the object, after the object is put.
	#[tangram_serialize(default, id = 8, skip_serializing_if = "Option::is_none")]
	pub account: Option<crate::usage::Account>,

	#[tangram_serialize(id = 0)]
	pub checkout: Option<tg::artifact::Id>,

	#[tangram_serialize(id = 1)]
	pub children: BTreeSet<tg::object::Id>,

	#[tangram_serialize(id = 2)]
	pub id: tg::object::Id,

	#[tangram_serialize(id = 3)]
	pub metadata: tg::object::Metadata,

	#[tangram_serialize(id = 7)]
	pub put: [u8; 16],

	#[tangram_serialize(id = 4)]
	pub storage: Set,

	#[tangram_serialize(id = 5)]
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
