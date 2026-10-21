use {crate::prelude::*, num::ToPrimitive as _};

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

	/// Build the graph, splitting its nodes into leaves and nesting the leaves in branches as checkin does when they do not fit in one object.
	#[must_use]
	pub fn build(self) -> tg::Graph {
		// Get the number of nodes in each leaf.
		let data = self
			.nodes
			.iter()
			.map(tg::graph::Node::to_data)
			.collect::<Vec<_>>();
		let counts = tg::graph::data::Leaf::counts(&data);

		// Create a single leaf if the nodes fit in one.
		if counts.len() == 1 {
			let leaf = tg::graph::Leaf { nodes: self.nodes };
			return tg::Graph::with_object(tg::graph::Object::Leaf(leaf));
		}

		// Create the leaves.
		let mut nodes = self.nodes.into_iter();
		let mut children = counts
			.into_iter()
			.map(|count| {
				let leaf = tg::graph::Leaf {
					nodes: nodes.by_ref().take(count).collect(),
				};
				let graph = tg::Graph::with_object(tg::graph::Object::Leaf(leaf));
				tg::graph::Child {
					count: count.to_u64().unwrap(),
					graph,
				}
			})
			.collect::<Vec<_>>();

		// Nest the children in branches until they fit in one branch.
		while children.len() > tg::graph::MAX_BRANCH_CHILDREN {
			children = children
				.chunks(tg::graph::MAX_BRANCH_CHILDREN)
				.map(|chunk| {
					let count = chunk.iter().map(|child| child.count).sum();
					let branch = tg::graph::Branch {
						children: chunk.to_vec(),
					};
					let graph = tg::Graph::with_object(tg::graph::Object::Branch(branch));
					tg::graph::Child { count, graph }
				})
				.collect();
		}

		// Create the root branch.
		let branch = tg::graph::Branch { children };
		tg::Graph::with_object(tg::graph::Object::Branch(branch))
	}
}
