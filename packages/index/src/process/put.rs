use {super::Stored, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub children: Option<Vec<tg::process::Id>>,
	pub command: tg::object::Id,
	pub error: Option<Option<Vec<tg::object::Id>>>,
	pub id: tg::process::Id,
	pub log: Option<Option<tg::object::Id>>,
	pub metadata: tg::process::Metadata,
	pub output: Option<Option<Vec<tg::object::Id>>>,
	pub parent: Option<tg::process::Id>,
	pub stored: Stored,
	pub touched_at: i64,
}

impl Arg {
	#[must_use]
	pub fn complete(&self) -> bool {
		self.set().complete()
			&& self.metadata.subtree.count.is_some()
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
