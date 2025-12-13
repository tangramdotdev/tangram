use {crate::Server, std::collections::HashMap, tangram_client::prelude::*};

struct State {
	dependencies: bool,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>,
	nodes: Vec<Option<tg::graph::data::Node>>,
	ids: Vec<tg::object::Id>,
	visited: HashMap<tg::artifact::Id, usize, tg::id::BuildHasher>,
}

impl Server {
	pub(super) fn checkout_write_lock(&self, state: &mut super::State) -> tg::Result<()> {
		// Create the lock.
		let lock = self
			.checkout_create_lock(&state.artifact, state.arg.dependencies)
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;

		// Do not write the lock if it is empty.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Write the lock.
		if state.artifact.is_directory() {
			let contents = serde_json::to_vec_pretty(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			let lockfile_path = state.path.join(tg::package::LOCKFILE_FILE_NAME);
			std::fs::write(&lockfile_path, &contents).map_err(
				|source| tg::error!(!source, path = %lockfile_path.display(), "failed to write the lockfile"),
			)?;
		} else if state.artifact.is_file() {
			let contents = serde_json::to_vec(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			xattr::set(&state.path, tg::file::LOCKATTR_XATTR_NAME, &contents)
				.map_err(|source| tg::error!(!source, "failed to write the lockattr"))?;
		}

		Ok(())
	}

	fn checkout_create_lock(
		&self,
		artifact: &tg::artifact::Id,
		dependencies: bool,
	) -> tg::Result<tg::graph::Data> {
		// Create the state.
		let mut state = State {
			dependencies,
			graphs: HashMap::default(),
			ids: Vec::new(),
			nodes: Vec::new(),
			visited: HashMap::default(),
		};

		// Create the nodes.
		let edge = tg::graph::data::Edge::Object(artifact.clone());
		self.checkout_create_lock_inner(&mut state, &edge)?;
		let nodes: Vec<_> = state
			.nodes
			.into_iter()
			.enumerate()
			.map(|(index, node)| {
				node.clone().ok_or_else(
					|| tg::error!(node = %index, "invalid graph, failed to create lock node"),
				)
			})
			.collect::<tg::Result<_>>()?;

		// Create the lock.
		let lock = tg::graph::Data { nodes };

		// Strip the lock.
		let lock = Self::strip_lock(lock);

		Ok(lock)
	}

	fn checkout_create_lock_inner(
		&self,
		state: &mut State,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<usize> {
		let (id, node, graph) = match edge {
			tg::graph::data::Edge::Reference(reference) => {
				// Load the graph.
				let graph = reference
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?;
				if !state.graphs.contains_key(graph) {
					let (_size, data) = self
						.store
						.try_get_object_data_sync(&graph.clone().into())?
						.ok_or_else(|| tg::error!("failed to load the graph"))?;
					let data = data
						.try_into()
						.map_err(|_| tg::error!("expected graph data"))?;
					state.graphs.insert(graph.clone(), data);
				}

				// Get the node.
				let node = state
					.graphs
					.get(graph)
					.unwrap()
					.nodes
					.get(reference.index)
					.ok_or_else(|| tg::error!("invalid node index"))?;

				// Compute the id.
				let data: tg::artifact::data::Artifact = match node.kind() {
					tg::artifact::Kind::Directory => {
						tg::directory::Data::Reference(reference.clone()).into()
					},
					tg::artifact::Kind::File => tg::file::Data::Reference(reference.clone()).into(),
					tg::artifact::Kind::Symlink => {
						tg::symlink::Data::Reference(reference.clone()).into()
					},
				};
				let id = tg::artifact::Id::new(node.kind(), &data.serialize()?);

				// Check if the node has been visited.
				if let Some(index) = state.visited.get(&id).copied() {
					return Ok(index);
				}

				(id, node.clone(), Some(graph.clone()))
			},
			tg::graph::data::Edge::Object(id) => {
				// Check if the node has been visited.
				if let Some(index) = state.visited.get(id).copied() {
					return Ok(index);
				}

				// Load the object.
				let (_size, data) = self
					.store
					.try_get_object_data_sync(&id.clone().into())?
					.ok_or_else(|| tg::error!("failed to load the object"))?;
				let data = data
					.try_into()
					.map_err(|_| tg::error!("expected artifact data"))?;

				match data {
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Reference(
						reference,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Reference(reference))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Reference(
						reference,
					)) => {
						let edge = tg::graph::data::Edge::Reference(reference);
						return self.checkout_create_lock_inner(state, &edge);
					},
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						(id.clone(), tg::graph::data::Node::Directory(node), None)
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						(id.clone(), tg::graph::data::Node::File(node), None)
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						(id.clone(), tg::graph::data::Node::Symlink(node), None)
					},
				}
			},
		};

		let index = state.nodes.len();
		state.visited.insert(id.clone(), index);
		state.ids.push(id.into());
		state.nodes.push(None);
		let node = match node {
			tg::graph::data::Node::Directory(node) => {
				self.checkout_create_lock_directory(state, node, graph.as_ref())?
			},
			tg::graph::data::Node::File(node) => {
				self.checkout_create_lock_file(state, node, graph.as_ref())?
			},
			tg::graph::data::Node::Symlink(node) => {
				self.checkout_create_lock_symlink(state, node, graph.as_ref())?
			},
		};
		state.nodes[index].replace(node);

		Ok(index)
	}

	fn checkout_create_lock_directory(
		&self,
		state: &mut State,
		node: tg::graph::data::Directory,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<tg::graph::data::Node> {
		let entries = node
			.entries
			.into_iter()
			.map(|(name, mut edge)| {
				if let tg::graph::data::Edge::Reference(reference) = &mut edge
					&& reference.graph.is_none()
				{
					reference.graph = graph.cloned();
				}
				let kind = edge.artifact_kind();
				let index = self.checkout_create_lock_inner(state, &edge)?;
				let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: None,
					index,
					kind,
				});
				Ok::<_, tg::Error>((name, edge))
			})
			.collect::<tg::Result<_>>()?;
		let directory = tg::graph::data::Directory { entries };
		let node = tg::graph::data::Node::Directory(directory);
		Ok(node)
	}

	fn checkout_create_lock_file(
		&self,
		state: &mut State,
		node: tg::graph::data::File,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<tg::graph::data::Node> {
		let dependencies = node
			.dependencies
			.into_iter()
			.map(|(reference, option)| {
				let Some(dependency) = option else {
					return Ok::<_, tg::Error>((reference, None));
				};
				let referent = dependency.0;
				let edge = match referent.item {
					Some(tg::graph::data::Edge::Reference(mut reference)) => {
						if reference.graph.is_none() {
							reference.graph = graph.cloned();
						}
						tg::graph::data::Edge::Reference(reference)
					},
					Some(tg::graph::data::Edge::Object(id)) => {
						let id = id
							.try_into()
							.map_err(|_| tg::error!("expected an artifact"))?;
						tg::graph::data::Edge::Object(id)
					},
					None => return Ok::<_, tg::Error>((reference, None)),
				};
				let kind = edge.artifact_kind();
				let index = self.checkout_create_lock_inner(state, &edge)?;
				let artifact = if state.dependencies {
					let id = state.ids[index].clone().try_into().unwrap();
					Some(id)
				} else {
					None
				};
				let item = Some(tg::graph::data::Edge::Reference(
					tg::graph::data::Reference {
						graph: None,
						index,
						kind,
					},
				));
				let options = tg::referent::Options {
					artifact,
					..referent.options
				};
				let referent = tg::Referent::new(item, options);
				Ok::<_, tg::Error>((reference, Some(tg::graph::data::Dependency(referent))))
			})
			.collect::<tg::Result<_>>()?;
		let file = tg::graph::data::File {
			contents: None,
			dependencies,
			executable: false,
		};
		let node = tg::graph::data::Node::File(file);
		Ok(node)
	}

	fn checkout_create_lock_symlink(
		&self,
		state: &mut State,
		node: tg::graph::data::Symlink,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<tg::graph::data::Node> {
		let artifact = node
			.artifact
			.as_ref()
			.map(|edge| {
				let mut edge = edge.clone();
				if let tg::graph::data::Edge::Reference(reference) = &mut edge
					&& reference.graph.is_none()
				{
					reference.graph = graph.cloned();
				}
				let kind = edge.artifact_kind();
				let node_index = self.checkout_create_lock_inner(state, &edge)?;
				Ok::<_, tg::Error>(tg::graph::data::Edge::Reference(
					tg::graph::data::Reference {
						graph: None,
						index: node_index,
						kind,
					},
				))
			})
			.transpose()?;
		let symlink = tg::graph::data::Symlink {
			artifact,
			path: node.path,
		};
		let node = tg::graph::data::Node::Symlink(symlink);
		Ok(node)
	}
}
