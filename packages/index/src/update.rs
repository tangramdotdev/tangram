use tangram_client::prelude::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
	Grant,
	Node,
	Storage,
}

#[derive(Clone, Debug, Default)]
pub struct Output {
	pub count: usize,
	pub processes_with_depth_exceeded: Vec<tg::process::Id>,
}

impl Output {
	pub fn merge(&mut self, other: Self) {
		self.count += other.count;
		self.processes_with_depth_exceeded
			.extend(other.processes_with_depth_exceeded);
	}
}
