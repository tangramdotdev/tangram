use {crate::store::Store, std::collections::BTreeMap, tangram_client as tg};

/// Collect all entries from a directory, recursively flattening branches.
pub fn collect_directory_entries(
	store: &Store,
	directory: &tg::graph::data::Directory,
	graph: Option<&tg::graph::Id>,
) -> tg::Result<BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>> {
	match directory {
		tg::graph::data::Directory::Leaf(leaf) => Ok(leaf.entries.clone()),
		tg::graph::data::Directory::Branch(branch) => {
			let mut all_entries = BTreeMap::new();
			for child in &branch.children {
				let child_dir = resolve_directory_child(store, &child.directory, graph)?;
				let child_entries = collect_directory_entries(store, &child_dir, graph)?;
				all_entries.extend(child_entries);
			}
			Ok(all_entries)
		},
	}
}

/// Resolve a directory child edge to its directory data.
fn resolve_directory_child(
	store: &Store,
	edge: &tg::graph::data::Edge<tg::directory::Id>,
	graph: Option<&tg::graph::Id>,
) -> tg::Result<tg::graph::data::Directory> {
	match edge {
		tg::graph::data::Edge::Object(id) => {
			// Load the directory data from the store.
			let (_size, data) = store
				.try_get_object_data_sync(&id.clone().into())
				.map_err(|source| tg::error!(!source, %id, "failed to get directory object"))?
				.ok_or_else(|| tg::error!(%id, "failed to find directory"))?;
			let dir_data: tg::directory::Data = data
				.try_into()
				.map_err(|_| tg::error!(%id, "expected directory data"))?;
			match dir_data {
				tg::directory::Data::Node(dir) => Ok(dir),
				tg::directory::Data::Pointer(_) => {
					Err(tg::error!("unexpected pointer in directory branch child"))
				},
			}
		},
		tg::graph::data::Edge::Pointer(pointer) => {
			// Get the directory from the graph.
			let child_graph_id = pointer
				.graph
				.as_ref()
				.or(graph)
				.ok_or_else(|| tg::error!("missing graph id for pointer"))?;
			let (_size, data) = store
				.try_get_object_data_sync(&child_graph_id.clone().into())
				.map_err(
					|source| tg::error!(!source, %child_graph_id, "failed to get graph object"),
				)?
				.ok_or_else(|| tg::error!(%child_graph_id, "failed to find graph"))?;
			let graph_data: tg::graph::Data = data
				.try_into()
				.map_err(|_| tg::error!(%child_graph_id, "expected graph data"))?;
			let node = graph_data
				.nodes
				.get(pointer.index)
				.ok_or_else(|| tg::error!("graph node index out of bounds"))?;
			match node {
				tg::graph::data::Node::Directory(dir) => Ok(dir.clone()),
				_ => Err(tg::error!("expected directory node in branch child")),
			}
		},
	}
}
