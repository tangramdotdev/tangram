use crate::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Builder {
	nodes: Vec<tg::graph::Node>,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn node(mut self, node: tg::graph::Node) -> Self {
		self.nodes.push(node);
		self
	}

	#[must_use]
	pub fn nodes(mut self, nodes: impl IntoIterator<Item = tg::graph::Node>) -> Self {
		self.nodes.extend(nodes);
		self
	}

	#[must_use]
	pub fn build(self) -> tg::Graph {
		tg::Graph::with_object(tg::graph::Object { nodes: self.nodes })
	}
}
