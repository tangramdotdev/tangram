use {super::Stored, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub children: Vec<tg::process::Id>,
	pub id: tg::process::Id,
	pub metadata: tg::process::Metadata,
	pub objects: Vec<(tg::object::Id, super::object::Kind)>,
	pub stored: Stored,
	pub touched_at: i64,
}

impl Arg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.metadata.subtree.count.is_some()
			&& self.metadata.subtree.command.complete()
			&& self.metadata.subtree.error.complete()
			&& self.metadata.subtree.log.complete()
			&& self.metadata.subtree.output.complete()
			&& self.metadata.node.command.complete()
			&& self.metadata.node.error.complete()
			&& self.metadata.node.log.complete()
			&& self.metadata.node.output.complete()
	}
}
