use tangram_client::prelude::*;

/// Resolve an internal edge using its containing graph.
pub fn resolve_edge<T>(
	cache: &crate::cache::Cache,
	edge: tg::graph::data::Edge<T>,
	graph: Option<&tg::graph::Id>,
) -> tg::Result<tg::graph::data::Edge<T>> {
	let tg::graph::data::Edge::Index(index) = edge else {
		return Ok(edge);
	};
	let graph = graph.ok_or_else(|| tg::error!("missing graph"))?;
	let node = get_node(cache, graph, index)?;
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
	graph: &tg::graph::Id,
	index: usize,
) -> tg::Result<tg::graph::data::Node> {
	let (_, data) = cache
		.try_get_object_data_sync(&graph.clone().into())
		.map_err(|error| tg::error!(!error, "failed to get the graph"))?
		.ok_or_else(|| tg::error!("missing graph"))?;
	let data: tg::graph::Data = data
		.try_into()
		.map_err(|_| tg::error!("expected a graph"))?;
	let node = data
		.nodes
		.get(index)
		.ok_or_else(|| tg::error!("invalid node index"))?
		.clone();
	Ok(node)
}
