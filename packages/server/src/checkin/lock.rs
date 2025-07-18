use crate::Server;
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) fn checkin_create_lock(state: &super::State) -> tg::Result<tg::graph::Data> {
		let mut nodes = Vec::with_capacity(state.graph.nodes.len());
		let mut objects = vec![None; state.graph.nodes.len()];
		let mut visited = vec![None; state.graph.nodes.len()];
		let root =
			Self::checkin_create_lock_node(state, 0, &mut nodes, &mut objects, &mut visited)?
				.unwrap_reference()
				.node;
		let nodes = nodes
			.into_iter()
			.map(|node| node.unwrap())
			.collect::<Vec<_>>();
		let nodes = crate::lock::strip(&nodes, &objects, root);
		Ok(tg::graph::Data { nodes })
	}

	pub(super) fn checkin_create_lock_node(
		state: &super::State,
		node: usize,
		nodes: &mut Vec<Option<tg::graph::data::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<tg::graph::data::Edge<tg::object::Id>>],
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		// Make sure parents are visited first.
		if let Some(parent) = state.graph.nodes[node].parent {
			Self::checkin_create_lock_node(state, parent, nodes, objects, visited)?;
		}

		// Check if this node is visited.
		if let Some(visited) = visited[node].clone() {
			return Ok(visited);
		}

		// If the object is not an artifact, return its ID.
		let id = state.graph.nodes[node].object.as_ref().unwrap().id.clone();
		if !(id.is_directory() || id.is_file() || id.is_symlink()) {
			visited[node].replace(tg::graph::data::Edge::Object(id.clone()));
			return Ok(tg::graph::data::Edge::Object(id));
		}

		// Get the artifact data.
		let Some(data) = state.graph.nodes[node]
			.object
			.as_ref()
			.unwrap()
			.data
			.clone()
		else {
			let id = state.graph.nodes[node].object.as_ref().unwrap().id.clone();
			return Ok(tg::graph::data::Edge::Object(id));
		};

		// Otherwise, create a slot for it.
		let index = nodes.len();
		visited[node].replace(tg::graph::data::Edge::Reference(
			tg::graph::data::Reference {
				graph: None,
				node: index,
			},
		));
		nodes.push(None);

		// Update the object ID.
		objects[index] = state.graph.nodes[node]
			.path
			.as_deref()
			.is_none_or(|path| path.strip_prefix(&state.artifacts_path).is_ok())
			.then(|| state.graph.nodes[node].object.as_ref().unwrap().id.clone());

		// Create the node.
		let data = data.try_into().unwrap();
		let lock_node = match &data {
			tg::artifact::Data::Directory(data) => {
				Self::checkin_create_lock_directory(state, node, data, nodes, objects, visited)?
			},
			tg::artifact::Data::File(data) => {
				Self::checkin_create_lock_file(state, node, data, nodes, objects, visited)?
			},
			tg::artifact::Data::Symlink(data) => {
				Self::checkin_create_lock_symlink(state, node, data, nodes, objects, visited)?
			},
		};

		nodes[index].replace(lock_node);
		Ok(tg::graph::data::Edge::Reference(
			tg::graph::data::Reference {
				graph: None,
				node: index,
			},
		))
	}

	pub(super) fn checkin_create_lock_directory(
		state: &super::State,
		node: usize,
		_data: &tg::directory::Data,
		nodes: &mut Vec<Option<tg::graph::data::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<tg::graph::data::Edge<tg::object::Id>>],
	) -> tg::Result<tg::graph::data::Node> {
		let entries = state.graph.nodes[node]
			.variant
			.unwrap_directory_ref()
			.entries
			.iter()
			.map(|(name, node)| {
				let edge = Self::checkin_create_lock_node(state, *node, nodes, objects, visited)?;
				let edge = match edge {
					tg::graph::data::Edge::Reference(reference) => {
						tg::graph::data::Edge::Reference(reference)
					},
					tg::graph::data::Edge::Object(object) => {
						tg::graph::data::Edge::Object(object.try_into()?)
					},
				};
				Ok::<_, tg::Error>((name.clone(), edge))
			})
			.collect::<tg::Result<_>>()?;
		let directory = tg::graph::data::Directory { entries };
		Ok(tg::graph::data::Node::Directory(directory))
	}

	pub(super) fn checkin_create_lock_file(
		state: &super::State,
		index: usize,
		data: &tg::file::Data,
		nodes: &mut Vec<Option<tg::graph::data::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<tg::graph::data::Edge<tg::object::Id>>],
	) -> tg::Result<tg::graph::data::Node> {
		let (contents, executable) = match data {
			tg::file::Data::Reference(data) => {
				let graph = &state
					.graph_objects
					.iter()
					.find(|object| object.id == data.graph.clone().unwrap())
					.unwrap()
					.data;
				let file = graph.nodes[data.node].clone().try_unwrap_file().unwrap();
				let contents = file
					.contents
					.ok_or_else(|| tg::error!("missing contents"))?;
				let executable = file.executable;
				(contents, executable)
			},
			tg::file::Data::Node(data) => (
				data.contents
					.clone()
					.ok_or_else(|| tg::error!("missing contents"))?,
				data.executable,
			),
		};
		let dependencies = state.graph.nodes[index]
			.variant
			.unwrap_file_ref()
			.dependencies
			.iter()
			.cloned()
			.map(|(reference, referent)| {
				let referent =
					referent.ok_or_else(|| tg::error!(%reference, "unresolved reference"))?;
				match referent.item().clone() {
					Either::Left(id) => {
						let item = tg::graph::data::Edge::Object(id);
						Ok((reference, referent.map(|_| item)))
					},
					Either::Right(node) => {
						let item =
							Self::checkin_create_lock_node(state, node, nodes, objects, visited)?;
						let path = referent
							.path()
							.cloned()
							.or_else(|| state.graph.referent_path(index, node));
						let tag = referent
							.tag()
							.cloned()
							.or_else(|| state.graph.nodes[node].tag.clone());
						let options = tg::referent::Options { path, tag };
						let referent = tg::Referent { item, options };
						Ok::<_, tg::Error>((reference, referent))
					},
				}
			})
			.collect::<tg::Result<_>>()?;
		let file = tg::graph::data::File {
			contents: Some(contents),
			dependencies,
			executable,
		};
		Ok(tg::graph::data::Node::File(file))
	}

	fn checkin_create_lock_symlink(
		state: &super::State,
		node: usize,
		data: &tg::symlink::Data,
		nodes: &mut Vec<Option<tg::graph::data::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<tg::graph::data::Edge<tg::object::Id>>],
	) -> tg::Result<tg::graph::data::Node> {
		// Get the artifact.
		let variant = &state.graph.nodes[node].variant.unwrap_symlink_ref();
		let artifact = variant
			.artifact
			.as_ref()
			.map(|artifact| match artifact {
				Either::Left(id) => Ok(tg::graph::data::Edge::Object(id.clone().into())),
				Either::Right(index) => {
					Self::checkin_create_lock_node(state, *index, nodes, objects, visited)
				},
			})
			.transpose()?;
		let artifact = if let Some(artifact) = artifact {
			Some(match artifact {
				tg::graph::data::Edge::Reference(reference) => {
					tg::graph::data::Edge::Reference(reference)
				},
				tg::graph::data::Edge::Object(object) => {
					tg::graph::data::Edge::Object(object.try_into()?)
				},
			})
		} else {
			None
		};

		// Get the path.
		let path = match data {
			tg::symlink::Data::Reference(data) => {
				let graph = &state
					.graph_objects
					.iter()
					.find(|object| object.id == data.graph.clone().unwrap())
					.unwrap()
					.data;
				let symlink = graph.nodes[data.node].clone().try_unwrap_symlink().unwrap();
				symlink.path
			},
			tg::symlink::Data::Node(data) => data.path.clone(),
		};

		// Create the lock node.
		Ok(tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
			artifact,
			path,
		}))
	}
}
