use {super::Storage, std::collections::BTreeSet, tangram_client::prelude::*};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub checkout: Option<tg::artifact::Id>,
	#[tangram_serialize(id = 1)]
	pub children: BTreeSet<tg::object::Id>,
	#[tangram_serialize(id = 2)]
	pub id: tg::object::Id,
	#[tangram_serialize(id = 3)]
	pub metadata: tg::object::Metadata,
	#[tangram_serialize(id = 4)]
	pub storage: Storage,
	#[tangram_serialize(id = 6)]
	pub touched_at: i64,
}

impl Arg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.metadata.subtree.complete()
	}
}
