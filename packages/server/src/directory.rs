use {std::collections::BTreeMap, tangram_client::prelude::*};

/// Collect all entries from a directory, recursively flattening branches.
pub fn collect_directory_entries(
	cache: &crate::cache::Cache,
	directory: &tg::graph::data::Directory,
	graph: Option<&tg::graph::Id>,
) -> tg::Result<BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>> {
	match directory {
		tg::graph::data::Directory::Leaf(leaf) => Ok(leaf.entries.clone()),
		tg::graph::data::Directory::Branch(branch) => {
			let mut all_entries = BTreeMap::new();
			for child in &branch.children {
				let (child_dir, child_graph) =
					resolve_directory_child(cache, &child.directory, graph)?;
				let child_entries =
					collect_directory_entries(cache, &child_dir, child_graph.as_ref())?;
				// Make edges explicit when moving entries out of their graph.
				for (name, edge) in child_entries {
					let edge = if child_graph.as_ref() == graph {
						edge
					} else {
						crate::graph::resolve_edge(cache, edge, child_graph.as_ref())?
					};
					all_entries.insert(name, edge);
				}
			}
			Ok(all_entries)
		},
	}
}

/// Resolve a directory child edge to its directory data.
fn resolve_directory_child(
	cache: &crate::cache::Cache,
	edge: &tg::graph::data::Edge<tg::directory::Id>,
	graph: Option<&tg::graph::Id>,
) -> tg::Result<(tg::graph::data::Directory, Option<tg::graph::Id>)> {
	let (graph, index) = match edge {
		tg::graph::data::Edge::Index(index) => {
			let graph = graph.ok_or_else(|| tg::error!("missing graph"))?;
			(graph, *index)
		},
		tg::graph::data::Edge::Pointer(pointer) => (&pointer.graph, pointer.index),
		tg::graph::data::Edge::Object(id) => {
			// Load the directory data from the cache.
			let (_size, data) = cache
				.try_get_object_data_sync(&id.clone().into())
				.map_err(|error| tg::error!(!error, %id, "failed to get directory object"))?
				.ok_or_else(|| tg::error!(%id, "failed to find directory"))?;
			let dir_data: tg::directory::Data = data
				.try_into()
				.map_err(|_| tg::error!(%id, "expected directory data"))?;
			match dir_data {
				tg::directory::Data::Node(dir) => return Ok((dir, None)),
				tg::directory::Data::Pointer(_) => {
					return Err(tg::error!("unexpected pointer in directory branch child"));
				},
			}
		},
	};
	let node = crate::graph::get_node(cache, graph, index)?;
	let directory = node
		.try_unwrap_directory()
		.map_err(|_| tg::error!("expected directory node in branch child"))?;
	Ok((directory, Some(graph.clone())))
}
