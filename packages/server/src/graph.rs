use {
	num::ToPrimitive as _,
	std::{
		collections::HashMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

/// The graphs that one operation has decoded, so that looking up many nodes of a graph decodes each of its objects once.
#[derive(Default)]
pub struct Graphs(Mutex<HashMap<tg::graph::Id, Arc<tg::graph::Data>, tg::id::BuildHasher>>);

/// Resolve an internal edge using its containing graph.
pub fn resolve_edge<T>(
	cache: &crate::cache::Cache,
	graphs: &Graphs,
	edge: tg::graph::data::Edge<T>,
	graph: Option<&tg::graph::Id>,
) -> tg::Result<tg::graph::data::Edge<T>> {
	let tg::graph::data::Edge::Index(index) = edge else {
		return Ok(edge);
	};
	let graph = graph.ok_or_else(|| tg::error!("missing graph"))?;
	let node = get_node(cache, graphs, graph, index)?;
	let pointer = tg::graph::data::Pointer {
		graph: graph.clone(),
		index,
		kind: node.kind(),
	};
	Ok(tg::graph::data::Edge::Pointer(pointer))
}

/// Load a graph node without resolving its edges, loading only the leaf that contains it.
pub fn get_node(
	cache: &crate::cache::Cache,
	graphs: &Graphs,
	graph: &tg::graph::Id,
	index: usize,
) -> tg::Result<tg::graph::data::Node> {
	let mut graph = graph.clone();
	let mut offset = index;
	loop {
		let data = graphs.get(cache, &graph)?;
		let branch = match data.as_ref() {
			tg::graph::Data::Leaf(leaf) => {
				let node = leaf
					.nodes
					.get(offset)
					.ok_or_else(|| tg::error!(%index, "invalid node index"))?
					.clone();
				return Ok(node);
			},
			tg::graph::Data::Branch(branch) => branch,
		};
		let mut child_graph = None;
		for child in &branch.children {
			let count = child.count.to_usize().unwrap();
			if offset < count {
				child_graph = Some(child.graph.clone());
				break;
			}
			offset -= count;
		}
		graph = child_graph.ok_or_else(|| tg::error!(%index, "invalid node index"))?;
	}
}

/// Load all of a graph's nodes in order as a single leaf, loading every leaf of a branched graph.
pub fn get_leaf(
	cache: &crate::cache::Cache,
	graphs: &Graphs,
	graph: &tg::graph::Id,
) -> tg::Result<tg::graph::data::Leaf> {
	let mut nodes = Vec::new();
	let mut stack = vec![graph.clone()];
	while let Some(graph) = stack.pop() {
		let data = graphs.get(cache, &graph)?;
		match data.as_ref() {
			tg::graph::Data::Leaf(leaf) => {
				nodes.extend(leaf.nodes.iter().cloned());
			},
			tg::graph::Data::Branch(branch) => {
				stack.extend(branch.children.iter().rev().map(|child| child.graph.clone()));
			},
		}
	}
	let leaf = tg::graph::data::Leaf { nodes };
	Ok(leaf)
}

impl Graphs {
	/// Get a graph object's data, decoding it if this operation has not already decoded it.
	pub fn get(
		&self,
		cache: &crate::cache::Cache,
		graph: &tg::graph::Id,
	) -> tg::Result<Arc<tg::graph::Data>> {
		if let Some(data) = self.0.lock().unwrap().get(graph) {
			return Ok(data.clone());
		}
		let (_, data) = cache
			.try_get_object_data_sync(&graph.clone().into())
			.map_err(|error| tg::error!(!error, %graph, "failed to get the graph"))?
			.ok_or_else(|| tg::error!(%graph, "missing graph"))?;
		let data: tg::graph::Data = data
			.try_into()
			.map_err(|_| tg::error!(%graph, "expected a graph"))?;
		let data = Arc::new(data);
		self.0.lock().unwrap().insert(graph.clone(), data.clone());
		Ok(data)
	}
}
