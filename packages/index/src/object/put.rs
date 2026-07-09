use {super::Stored, std::collections::BTreeSet, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub cache_entry: Option<tg::artifact::Id>,
	pub children: BTreeSet<tg::object::Id>,
	pub id: tg::object::Id,
	pub metadata: tg::object::Metadata,
	pub stored: Stored,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

impl Arg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.metadata.subtree.complete()
	}
}
