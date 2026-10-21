use {
	std::{
		collections::HashMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

/// The graphs that one operation has decoded, so that looking up many nodes of a graph decodes it once.
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

/// Load a graph node without resolving its edges.
pub fn get_node(
	cache: &crate::cache::Cache,
	graphs: &Graphs,
	graph: &tg::graph::Id,
	index: usize,
) -> tg::Result<tg::graph::data::Node> {
	let data = graphs.get(cache, graph)?;
	let node = data
		.nodes
		.get(index)
		.ok_or_else(|| tg::error!("invalid node index"))?
		.clone();
	Ok(node)
}

impl Graphs {
	/// Get a graph's data, decoding it if this operation has not already decoded it.
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
			.map_err(|error| tg::error!(!error, "failed to get the graph"))?
			.ok_or_else(|| tg::error!("missing graph"))?;
		let data: tg::graph::Data = data
			.try_into()
			.map_err(|_| tg::error!("expected a graph"))?;
		let data = Arc::new(data);
		self.0.lock().unwrap().insert(graph.clone(), data.clone());
		Ok(data)
	}
}
