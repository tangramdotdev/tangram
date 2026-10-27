use {
	crate::{Graph, Variant},
	tangram_util::iter::Ext as _,
};

pub(crate) struct Petgraph<'a> {
	pub graph: &'a Graph,
	pub next: usize,
}

impl petgraph::visit::GraphBase for Petgraph<'_> {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for &'a Petgraph<'a> {
	type NodeIdentifiers = Box<dyn Iterator<Item = usize> + 'a>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		self.graph
			.nodes
			.range(self.next..)
			.filter(|(_, node)| !node.variant.is_object())
			.map(|(index, _)| index)
			.copied()
			.boxed()
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a Petgraph<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		let Some(node) = self.graph.nodes.get(&id) else {
			return std::iter::empty().boxed();
		};
		let next = self.next;
		match &node.variant {
			Variant::Directory(directory) => directory
				.entries
				.values()
				.filter_map(move |edge| {
					edge.try_unwrap_index_ref()
						.ok()
						.copied()
						.filter(|&index| index >= next)
				})
				.boxed(),
			Variant::File(file) => file
				.dependencies
				.values()
				.filter_map(move |option| {
					option
						.as_ref()
						.and_then(|referent| referent.node.as_ref())
						.and_then(|edge| edge.try_unwrap_index_ref().ok().copied())
						.filter(|&index| index >= next)
				})
				.boxed(),
			Variant::Object => std::iter::empty().boxed(),
			Variant::Symlink(symlink) => symlink
				.artifact
				.iter()
				.filter_map(move |edge| {
					edge.try_unwrap_index_ref()
						.ok()
						.copied()
						.filter(|&index| index >= next)
				})
				.boxed(),
		}
	}
}

impl petgraph::visit::NodeIndexable for &Petgraph<'_> {
	fn node_bound(&self) -> usize {
		self.graph.next
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}

	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}
}
