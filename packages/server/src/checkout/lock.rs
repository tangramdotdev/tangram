use {
	crate::Server,
	std::collections::{HashMap, HashSet},
	tangram_client::prelude::*,
};

struct State {
	dependencies: bool,
	ids: Vec<tg::object::Id>,
	nodes: Vec<Option<tg::graph::data::Node>>,
	visited: HashMap<tg::artifact::Id, usize, tg::id::BuildHasher>,
	visited_graphs: HashSet<tg::graph::Id, tg::id::BuildHasher>,
}

impl Server {
	pub(super) fn checkout_write_lock(&self, state: &mut super::State) -> tg::Result<()> {
		// Do not write a lock if the lock arg is not set.
		if state.arg.lock.is_none() {
			return Ok(());
		}

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
			let lockfile_path = state.path.join(tg::module::LOCKFILE_FILE_NAME);
			std::fs::write(&lockfile_path, &contents).map_err(
				|source| tg::error!(!source, path = %lockfile_path.display(), "failed to write the lockfile"),
			)?;
		} else if state.artifact.is_file() {
			match state.arg.lock {
				Some(tg::checkout::Lock::File) => {
					let contents = serde_json::to_vec_pretty(&lock)
						.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
					let lockfile_path = state.path.with_extension("lock");
					std::fs::write(&lockfile_path, &contents).map_err(
						|source| tg::error!(!source, path = %lockfile_path.display(), "failed to write the lockfile"),
					)?;
				},
				Some(tg::checkout::Lock::Attr) => {
					let contents = serde_json::to_vec(&lock)
						.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
					xattr::set(&state.path, tg::file::LOCKATTR_XATTR_NAME, &contents)
						.map_err(|source| tg::error!(!source, "failed to write the lockattr"))?;
				},
				None => unreachable!(),
			}
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
			ids: Vec::new(),
			nodes: Vec::new(),
			visited: HashMap::default(),
			visited_graphs: HashSet::default(),
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

		// Strip the lock. Pass false for local_dependencies since checkout always includes
		// dependencies with local option in the lock.
		let lock = Self::strip_lock(lock, false);

		Ok(lock)
	}

	fn checkout_create_lock_inner(
		&self,
		state: &mut State,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<usize> {
		let (id, node, graph) = match edge {
			tg::graph::data::Edge::Pointer(pointer) => {
				// Get the graph ID.
				let graph_id = pointer
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?
					.clone();

				// If this graph has not been visited, process ALL nodes in the graph.
				if state.visited_graphs.insert(graph_id.clone()) {
					// Load the graph data.
					let (_size, data) = self
						.store
						.try_get_object_data_sync(&graph_id.clone().into())
						.map_err(
							|source| tg::error!(!source, %graph_id, "failed to get the graph object"),
						)?
						.ok_or_else(|| tg::error!(%graph_id, "failed to find the graph"))?;
					let graph_data: tg::graph::Data = data
						.try_into()
						.map_err(|_| tg::error!(%graph_id, "expected graph data"))?;

					// Process all nodes in the graph.
					self.checkout_create_lock_graph(state, &graph_id, &graph_data)?;
				}

				// Compute the artifact ID using the pointer kind.
				let data: tg::artifact::data::Artifact = match pointer.kind {
					tg::artifact::Kind::Directory => {
						tg::directory::Data::Pointer(pointer.clone()).into()
					},
					tg::artifact::Kind::File => tg::file::Data::Pointer(pointer.clone()).into(),
					tg::artifact::Kind::Symlink => {
						tg::symlink::Data::Pointer(pointer.clone()).into()
					},
				};
				let id = tg::artifact::Id::new(pointer.kind, &data.serialize()?);

				// The node should now be in state.visited since we processed the graph.
				let index = state
					.visited
					.get(&id)
					.copied()
					.ok_or_else(|| tg::error!("node not found after processing graph"))?;

				return Ok(index);
			},
			tg::graph::data::Edge::Object(id) => {
				// Check if the node has been visited.
				if let Some(index) = state.visited.get(id).copied() {
					return Ok(index);
				}

				// Load the object.
				let (_size, data) = self
					.store
					.try_get_object_data_sync(&id.clone().into())
					.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
					.ok_or_else(|| tg::error!(%id, "failed to find the object"))?;
				let data = data
					.try_into()
					.map_err(|_| tg::error!(%id, "expected artifact data"))?;

				match data {
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Pointer(
						pointer,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Pointer(pointer))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Pointer(pointer)) => {
						let edge = tg::graph::data::Edge::Pointer(pointer);
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
				self.checkout_create_lock_directory(state, &node, graph.as_ref())?
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
		node: &tg::graph::data::Directory,
		graph: Option<&tg::graph::Id>,
	) -> tg::Result<tg::graph::data::Node> {
		// Collect all entries from the directory, flattening branches.
		let all_entries = crate::directory::collect_directory_entries(&self.store, node, graph)?;

		// Transform each entry for the lock.
		let entries = all_entries
			.into_iter()
			.map(|(name, mut edge)| {
				if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
					&& pointer.graph.is_none()
				{
					pointer.graph = graph.cloned();
				}
				let kind = edge.artifact_kind();
				let index = self.checkout_create_lock_inner(state, &edge)?;
				let edge = tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index,
					kind,
				});
				Ok::<_, tg::Error>((name, edge))
			})
			.collect::<tg::Result<_>>()?;

		let leaf = tg::graph::data::DirectoryLeaf { entries };
		let directory = tg::graph::data::Directory::Leaf(leaf);
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
					Some(tg::graph::data::Edge::Pointer(mut pointer)) => {
						if pointer.graph.is_none() {
							pointer.graph = graph.cloned();
						}
						tg::graph::data::Edge::Pointer(pointer)
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
				let item = Some(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index,
					kind,
				}));
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
			executable: node.executable,
			module: node.module,
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
				if let tg::graph::data::Edge::Pointer(pointer) = &mut edge
					&& pointer.graph.is_none()
				{
					pointer.graph = graph.cloned();
				}
				let kind = edge.artifact_kind();
				let node_index = self.checkout_create_lock_inner(state, &edge)?;
				Ok::<_, tg::Error>(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index: node_index,
					kind,
				}))
			})
			.transpose()?;
		let symlink = tg::graph::data::Symlink {
			artifact,
			path: node.path,
		};
		let node = tg::graph::data::Node::Symlink(symlink);
		Ok(node)
	}

	fn checkout_create_lock_graph(
		&self,
		state: &mut State,
		graph_id: &tg::graph::Id,
		graph_data: &tg::graph::Data,
	) -> tg::Result<HashMap<usize, usize, fnv::FnvBuildHasher>> {
		// Maps graph_node_index -> lock_node_index.
		let mut graph_to_lock: HashMap<usize, usize, fnv::FnvBuildHasher> = HashMap::default();

		// First pass: allocate lock indices for all graph nodes.
		for (graph_index, node) in graph_data.nodes.iter().enumerate() {
			let pointer = tg::graph::data::Pointer {
				graph: Some(graph_id.clone()),
				index: graph_index,
				kind: node.kind(),
			};
			let data: tg::artifact::data::Artifact = match node.kind() {
				tg::artifact::Kind::Directory => {
					tg::directory::Data::Pointer(pointer.clone()).into()
				},
				tg::artifact::Kind::File => tg::file::Data::Pointer(pointer.clone()).into(),
				tg::artifact::Kind::Symlink => tg::symlink::Data::Pointer(pointer.clone()).into(),
			};
			let id = tg::artifact::Id::new(node.kind(), &data.serialize()?);

			// Check if this node was already visited via a different path.
			if let Some(&lock_index) = state.visited.get(&id) {
				graph_to_lock.insert(graph_index, lock_index);
			} else {
				let lock_index = state.nodes.len();
				state.visited.insert(id.clone(), lock_index);
				state.ids.push(id.into());
				state.nodes.push(None);
				graph_to_lock.insert(graph_index, lock_index);
			}
		}

		// Second pass: fill in the nodes.
		for (graph_index, node) in graph_data.nodes.iter().enumerate() {
			let lock_index = graph_to_lock[&graph_index];
			if state.nodes[lock_index].is_some() {
				// Already filled in (was visited before this graph).
				continue;
			}

			let lock_node = match node {
				tg::graph::data::Node::Directory(dir) => {
					self.checkout_create_lock_directory_graph(state, dir, graph_id, &graph_to_lock)?
				},
				tg::graph::data::Node::File(file) => self.checkout_create_lock_file_graph(
					state,
					file.clone(),
					graph_id,
					&graph_to_lock,
				)?,
				tg::graph::data::Node::Symlink(sym) => self.checkout_create_lock_symlink_graph(
					state,
					sym.clone(),
					graph_id,
					&graph_to_lock,
				)?,
			};
			state.nodes[lock_index].replace(lock_node);
		}

		Ok(graph_to_lock)
	}

	fn checkout_create_lock_directory_graph(
		&self,
		state: &mut State,
		node: &tg::graph::data::Directory,
		graph_id: &tg::graph::Id,
		graph_to_lock: &HashMap<usize, usize, fnv::FnvBuildHasher>,
	) -> tg::Result<tg::graph::data::Node> {
		// Collect all entries, flattening branches recursively.
		let all_entries =
			crate::directory::collect_directory_entries(&self.store, node, Some(graph_id))?;

		// Process entries to create lock pointers.
		let entries = all_entries
			.into_iter()
			.map(|(name, edge)| {
				let (kind, index) = match &edge {
					tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
						let index = graph_to_lock[&pointer.index];
						(pointer.kind, index)
					},
					tg::graph::data::Edge::Pointer(pointer) => {
						let mut pointer = pointer.clone();
						if pointer.graph.is_none() {
							pointer.graph = Some(graph_id.clone());
						}
						let edge = tg::graph::data::Edge::Pointer(pointer.clone());
						let kind = edge.artifact_kind();
						let index = self.checkout_create_lock_inner(state, &edge)?;
						(kind, index)
					},
					tg::graph::data::Edge::Object(_) => {
						let kind = edge.artifact_kind();
						let index = self.checkout_create_lock_inner(state, &edge)?;
						(kind, index)
					},
				};
				let edge = tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index,
					kind,
				});
				Ok::<_, tg::Error>((name, edge))
			})
			.collect::<tg::Result<_>>()?;
		let leaf = tg::graph::data::DirectoryLeaf { entries };
		let directory = tg::graph::data::Directory::Leaf(leaf);
		let node = tg::graph::data::Node::Directory(directory);
		Ok(node)
	}

	fn checkout_create_lock_file_graph(
		&self,
		state: &mut State,
		node: tg::graph::data::File,
		graph_id: &tg::graph::Id,
		graph_to_lock: &HashMap<usize, usize, fnv::FnvBuildHasher>,
	) -> tg::Result<tg::graph::data::Node> {
		let dependencies = node
			.dependencies
			.into_iter()
			.map(|(reference, option)| {
				let Some(dependency) = option else {
					return Ok::<_, tg::Error>((reference, None));
				};
				let referent = dependency.0;
				let (kind, index) = match referent.item {
					Some(tg::graph::data::Edge::Pointer(pointer)) if pointer.graph.is_none() => {
						let index = graph_to_lock[&pointer.index];
						(pointer.kind, index)
					},
					Some(tg::graph::data::Edge::Pointer(mut pointer)) => {
						if pointer.graph.is_none() {
							pointer.graph = Some(graph_id.clone());
						}
						let edge = tg::graph::data::Edge::Pointer(pointer.clone());
						let kind = edge.artifact_kind();
						let index = self.checkout_create_lock_inner(state, &edge)?;
						(kind, index)
					},
					Some(tg::graph::data::Edge::Object(id)) => {
						let id = id
							.try_into()
							.map_err(|_| tg::error!("expected an artifact"))?;
						let edge = tg::graph::data::Edge::Object(id);
						let kind = edge.artifact_kind();
						let index = self.checkout_create_lock_inner(state, &edge)?;
						(kind, index)
					},
					None => return Ok::<_, tg::Error>((reference, None)),
				};
				let artifact = if state.dependencies {
					let id = state.ids[index].clone().try_into().unwrap();
					Some(id)
				} else {
					None
				};
				let item = Some(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index,
					kind,
				}));
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
			executable: node.executable,
			module: node.module,
		};
		let node = tg::graph::data::Node::File(file);
		Ok(node)
	}

	fn checkout_create_lock_symlink_graph(
		&self,
		state: &mut State,
		node: tg::graph::data::Symlink,
		graph_id: &tg::graph::Id,
		graph_to_lock: &HashMap<usize, usize, fnv::FnvBuildHasher>,
	) -> tg::Result<tg::graph::data::Node> {
		let artifact = node
			.artifact
			.as_ref()
			.map(|edge| {
				let (kind, index) = match edge {
					tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
						let index = graph_to_lock[&pointer.index];
						(pointer.kind, index)
					},
					tg::graph::data::Edge::Pointer(pointer) => {
						let mut pointer = pointer.clone();
						if pointer.graph.is_none() {
							pointer.graph = Some(graph_id.clone());
						}
						let edge = tg::graph::data::Edge::Pointer(pointer.clone());
						let kind = edge.artifact_kind();
						let index = self.checkout_create_lock_inner(state, &edge)?;
						(kind, index)
					},
					tg::graph::data::Edge::Object(_) => {
						let kind = edge.artifact_kind();
						let index = self.checkout_create_lock_inner(state, edge)?;
						(kind, index)
					},
				};
				Ok::<_, tg::Error>(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index,
					kind,
				}))
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
