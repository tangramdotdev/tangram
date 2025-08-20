use crate::Server;
use std::collections::HashMap;
use tangram_client as tg;

struct State {
	dependencies: bool,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, fnv::FnvBuildHasher>,
	nodes: Vec<Option<tg::graph::data::Node>>,
	visited: HashMap<tg::artifact::Id, usize, fnv::FnvBuildHasher>,
}

impl Server {
	pub(super) fn checkout_write_lock(
		&self,
		id: tg::artifact::Id,
		state: &mut super::State,
	) -> tg::Result<()> {
		// Create the lock.
		let lock = self
			.checkout_create_lock(&id, state.arg.dependencies)
			.map_err(|source| tg::error!(!source, "failed to create the lock"))?;

		// Do not write the lock if it is empty.
		if lock.nodes.is_empty() {
			return Ok(());
		}

		// Write the lock.
		let artifact = tg::Artifact::with_id(id);
		if artifact.is_directory() {
			let contents = serde_json::to_vec_pretty(&lock)
				.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
			let lockfile_path = state.path.join(tg::package::LOCKFILE_FILE_NAME);
			std::fs::write(&lockfile_path, &contents).map_err(
				|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write the lockfile"),
			)?;
		} else if artifact.is_file() {
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
			nodes: Vec::new(),
			visited: HashMap::default(),
		};

		// Create nodes in the lock for the graph.
		let edge = tg::graph::data::Edge::Object(artifact.clone());
		self.checkout_create_lock_inner(&mut state, &edge)?;
		let nodes: Vec<_> = state
			.nodes
			.into_iter()
			.enumerate()
			.map(|(index, node)| {
				node.ok_or_else(
					|| tg::error!(%node = index, "invalid graph, failed to create lock node"),
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
				// Compute the ID.
				let graph = reference
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?;
				self.checkout_create_lock_ensure_graph_exists(state, graph)?;

				// Get the node data.
				let node = state
					.graphs
					.get(graph)
					.unwrap()
					.nodes
					.get(reference.node)
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

				// Check if visited.
				if let Some(index) = state.visited.get(&id).copied() {
					return Ok(index);
				}

				(id, node.clone(), Some(graph.clone()))
			},
			tg::graph::data::Edge::Object(id) => {
				// Check if visited.
				if let Some(index) = state.visited.get(id).copied() {
					return Ok(index);
				}

				// Otherwise, lookup the artifact data by ID.
				#[allow(clippy::match_wildcard_for_single_variants)]
				let data = match &self.store {
					crate::Store::Lmdb(store) => {
						store.try_get_object_data_sync(&id.clone().into())?
					},
					crate::Store::Memory(store) => store.try_get_object_data(&id.clone().into())?,
					_ => {
						return Err(tg::error!("unimplemented"));
					},
				}
				.ok_or_else(|| tg::error!(%id = id.clone(), "expected the object to be stored"))?;
				let data = tg::artifact::Data::try_from(data)?;

				match data {
					// Handle the case where this points into a graph.
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
		state.nodes.push(None);
		let lock_node = match node {
			tg::graph::data::Node::Directory(node) => {
				self.checkout_create_lock_directory(state, &node, graph.as_ref())?
			},
			tg::graph::data::Node::File(node) => {
				self.checkout_create_lock_file(state, &node, graph.as_ref())?
			},
			tg::graph::data::Node::Symlink(node) => {
				self.checkout_create_lock_symlink(state, &node, graph.as_ref())?
			},
		};
		state.nodes[index].replace(lock_node);
		Ok(index)
	}

	fn checkout_create_lock_ensure_graph_exists(
		&self,
		state: &mut State,
		graph: &tg::graph::Id,
	) -> tg::Result<()> {
		if state.graphs.contains_key(graph) {
			return Ok(());
		}
		#[allow(clippy::match_wildcard_for_single_variants)]
		let data = match &self.store {
			crate::Store::Lmdb(store) => store.try_get_object_data_sync(&graph.clone().into())?,
			crate::Store::Memory(store) => store.try_get_object_data(&graph.clone().into())?,
			_ => {
				return Err(tg::error!("unimplemented"));
			},
		}
		.ok_or_else(|| tg::error!("expected the object to be stored"))?
		.try_into()
		.map_err(|_| tg::error!("expected a graph"))?;
		state.graphs.insert(graph.clone(), data);
		Ok(())
	}

	fn checkout_create_lock_directory(
		&self,
		state: &mut State,
		node: &tg::graph::data::Directory,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<tg::graph::data::Node> {
		let entries = node
			.entries
			.iter()
			.map(|(name, edge)| {
				let mut edge = edge.clone();
				if let tg::graph::data::Edge::Reference(reference) = &mut edge {
					if reference.graph.is_none() {
						reference.graph = graph.cloned();
					}
				}
				let node = self.checkout_create_lock_inner(state, &edge)?;
				let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: None,
					node,
				});
				Ok::<_, tg::Error>((name.clone(), edge))
			})
			.collect::<tg::Result<_>>()?;
		let directory = tg::graph::data::Directory { entries };
		let node = tg::graph::data::Node::Directory(directory);
		Ok(node)
	}

	fn checkout_create_lock_file(
		&self,
		state: &mut State,
		node: &tg::graph::data::File,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<tg::graph::data::Node> {
		let dependencies = node
			.dependencies
			.iter()
			.map(|(reference, referent)| {
				let edge = match referent.item.clone() {
					tg::graph::data::Edge::Reference(mut reference) => {
						if reference.graph.is_none() {
							reference.graph = graph.cloned();
						}
						tg::graph::data::Edge::Reference(reference)
					},
					tg::graph::data::Edge::Object(tg::object::Id::Directory(id))
						if state.dependencies =>
					{
						tg::graph::data::Edge::Object(id.into())
					},
					tg::graph::data::Edge::Object(tg::object::Id::File(id))
						if state.dependencies =>
					{
						tg::graph::data::Edge::Object(id.into())
					},
					tg::graph::data::Edge::Object(tg::object::Id::Symlink(id))
						if state.dependencies =>
					{
						tg::graph::data::Edge::Object(id.into())
					},
					tg::graph::data::Edge::Object(id) => {
						tg::graph::data::Edge::Object(id.try_into()?)
					},
				};
				let node = self.checkout_create_lock_inner(state, &edge)?;
				let referent = referent.clone().map(|_| {
					tg::graph::data::Edge::Reference(tg::graph::data::Reference {
						graph: None,
						node,
					})
				});
				Ok::<_, tg::Error>((reference.clone(), referent))
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
		node: &tg::graph::data::Symlink,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<tg::graph::data::Node> {
		let artifact = node
			.artifact
			.as_ref()
			.map(|edge| {
				let mut edge = edge.clone();
				if let tg::graph::data::Edge::Reference(reference) = &mut edge {
					if reference.graph.is_none() {
						reference.graph = graph.cloned();
					}
				}
				self.checkout_create_lock_inner(state, &edge)
			})
			.transpose()?
			.map(|node| {
				tg::graph::data::Edge::Reference(tg::graph::data::Reference { graph: None, node })
			});
		let symlink = tg::graph::data::Symlink {
			artifact,
			path: None,
		};
		let node = tg::graph::data::Node::Symlink(symlink);
		Ok(node)
	}
}
