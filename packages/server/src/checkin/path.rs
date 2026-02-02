use {
	super::graph::{Graph, Variant},
	crate::Server,
	std::{collections::BTreeMap, path::Path},
	tangram_client as tg,
};

pub type Paths = BTreeMap<(usize, tg::Reference), tg::graph::data::Edge<tg::object::Id>>;

impl Server {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_path_get_edges(
		&self,
		graph: &Graph,
		next: usize,
	) -> tg::Result<Paths> {
		let mut paths = Paths::new();
		for index in next..graph.next {
			let node = graph.nodes.get(&index).unwrap();
			if let Variant::File(file) = &node.variant {
				for (reference, dependency) in &file.dependencies {
					if let Some(path) = reference.options().path.as_ref()
						&& let Some(dependency) = dependency
						&& let Some(edge) = dependency.item()
					{
						let visit = if let Some(id) = dependency.id() {
							Self::checkin_path_edge_matches_id(graph, edge, id)
						} else {
							Self::checkin_path_edge_is_directory(graph, edge)
						};
						if visit {
							let target = self.checkin_path_visit(graph, edge.clone(), path).await?;
							paths.insert((index, reference.clone()), target);
						}
					}
				}
			}
		}
		Ok(paths)
	}

	fn checkin_path_edge_matches_id(
		graph: &Graph,
		edge: &tg::graph::data::Edge<tg::object::Id>,
		id: &tg::object::Id,
	) -> bool {
		let (is_directory, node_id) = match edge {
			tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
				let node = graph.nodes.get(&pointer.index);
				let is_directory = matches!(pointer.kind, tg::artifact::Kind::Directory);
				let node_id = node.and_then(|node| node.id.clone());
				(is_directory, node_id)
			},
			tg::graph::data::Edge::Pointer(p) => {
				(matches!(p.kind, tg::artifact::Kind::Directory), None)
			},
			tg::graph::data::Edge::Object(edge_id) => return edge_id == id,
		};
		let options_is_directory = id.kind() == tg::object::Kind::Directory;
		if options_is_directory && !is_directory {
			return false;
		}
		node_id.is_none_or(|id_| &id_ == id)
	}

	fn checkin_path_edge_is_directory(
		graph: &Graph,
		edge: &tg::graph::data::Edge<tg::object::Id>,
	) -> bool {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => graph
				.nodes
				.get(&pointer.index)
				.is_some_and(|n| matches!(&n.variant, Variant::Directory(_))),
			tg::graph::data::Edge::Pointer(pointer) => {
				matches!(pointer.kind, tg::artifact::Kind::Directory)
			},
			tg::graph::data::Edge::Object(id) => id.kind() == tg::object::Kind::Directory,
		}
	}

	async fn checkin_path_visit(
		&self,
		graph: &Graph,
		edge: tg::graph::data::Edge<tg::object::Id>,
		path: &Path,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		let mut current = edge;
		for component in path.components() {
			let name = match component {
				std::path::Component::Normal(n) => n
					.to_str()
					.ok_or_else(|| tg::error!("invalid path component"))?,
				_ => return Err(tg::error!("unexpected path component")),
			};
			current = self
				.checkin_path_get_directory_entry(graph, &current, name)
				.await?;
		}
		Ok(current)
	}

	async fn checkin_path_get_directory_entry(
		&self,
		graph: &Graph,
		edge: &tg::graph::data::Edge<tg::object::Id>,
		name: &str,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
				let node = graph
					.nodes
					.get(&pointer.index)
					.ok_or_else(|| tg::error!("node not found"))?;
				let directory = node
					.variant
					.try_unwrap_directory_ref()
					.map_err(|_| tg::error!("expected a directory"))?;
				let entry = directory
					.entries
					.get(name)
					.ok_or_else(|| tg::error!(%name, "entry not found"))?;
				Ok(Self::checkin_path_convert_artifact_edge(entry))
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				let graph_id = pointer
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("expected graph id"))?;
				let data = self.checkin_path_load_graph(graph_id).await?;
				let node = data
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let directory = node
					.try_unwrap_directory_ref()
					.map_err(|_| tg::error!("expected a directory"))?;
				let leaf = directory
					.try_unwrap_leaf_ref()
					.map_err(|_| tg::error!("expected a leaf directory"))?;
				let entry = leaf
					.entries
					.get(name)
					.ok_or_else(|| tg::error!(%name, "entry not found"))?;
				Ok(Self::checkin_path_convert_entry_with_graph(entry, graph_id))
			},
			tg::graph::data::Edge::Object(id) => {
				let artifact_id: tg::artifact::Id = id
					.clone()
					.try_into()
					.map_err(|_| tg::error!("expected artifact"))?;
				let data = self.checkin_path_load_directory(&artifact_id).await?;
				let leaf = data
					.try_unwrap_leaf_ref()
					.map_err(|_| tg::error!("expected a leaf directory"))?;
				let entry = leaf
					.entries
					.get(name)
					.ok_or_else(|| tg::error!(%name, "entry not found"))?;
				Ok(Self::checkin_path_convert_artifact_edge(entry))
			},
		}
	}

	async fn checkin_path_load_graph(&self, id: &tg::graph::Id) -> tg::Result<tg::graph::Data> {
		let output = self
			.try_get_object_local(&id.clone().into(), false)
			.await?
			.ok_or_else(|| tg::error!("graph not found"))?;
		tg::graph::Data::deserialize(output.bytes)
			.map_err(|e| tg::error!(!e, "failed to deserialize graph"))
	}

	async fn checkin_path_load_directory(
		&self,
		id: &tg::artifact::Id,
	) -> tg::Result<tg::graph::data::Directory> {
		let output = self
			.try_get_object_local(&id.clone().into(), false)
			.await?
			.ok_or_else(|| tg::error!("directory not found"))?;
		let data = tg::directory::Data::deserialize(output.bytes)
			.map_err(|e| tg::error!(!e, "failed to deserialize"))?;
		match data {
			tg::directory::Data::Node(node) => Ok(node),
			tg::directory::Data::Pointer(pointer) => {
				let graph_id = pointer
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("expected graph"))?;
				let graph = self.checkin_path_load_graph(graph_id).await?;
				let node = graph
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				node.try_unwrap_directory_ref()
					.cloned()
					.map_err(|_| tg::error!("expected a directory"))
			},
		}
	}

	fn checkin_path_convert_artifact_edge(
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::graph::data::Edge<tg::object::Id> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) => {
				tg::graph::data::Edge::Pointer(pointer.clone())
			},
			tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.clone().into()),
		}
	}

	fn checkin_path_convert_entry_with_graph(
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		graph_id: &tg::graph::Id,
	) -> tg::graph::data::Edge<tg::object::Id> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) => {
				let graph = pointer.graph.clone().or_else(|| Some(graph_id.clone()));
				tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph,
					index: pointer.index,
					kind: pointer.kind,
				})
			},
			tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.clone().into()),
		}
	}
}
