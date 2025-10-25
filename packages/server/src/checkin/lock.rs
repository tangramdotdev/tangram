use {
	super::state::{State, Variant},
	crate::Server,
	std::{collections::BTreeMap, path::Path},
	tangram_client as tg,
	tangram_util::iter::Ext as _,
};

impl Server {
	pub(crate) fn checkin_try_read_lock(path: &Path) -> tg::Result<Option<tg::graph::Data>> {
		// Attempt to read a lockattr.
		let contents = 'a: {
			let Ok(Some(contents)) = xattr::get(path, tg::file::LOCKATTR_XATTR_NAME) else {
				break 'a None;
			};
			Some(contents)
		};

		// Attempt to read a lockfile.
		let contents = 'a: {
			if let Some(contents) = contents {
				break 'a Some(contents);
			}
			let lock_path = path.join(tg::package::LOCKFILE_FILE_NAME);
			let contents = match std::fs::read(&lock_path) {
				Ok(contents) => contents,
				Err(error)
					if matches!(
						error.kind(),
						std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
					) =>
				{
					break 'a None;
				},
				Err(source) => {
					return Err(
						tg::error!(!source, %path = lock_path.display(), "failed to read the lockfile"),
					);
				},
			};
			Some(contents)
		};

		let Some(contents) = contents else {
			return Ok(None);
		};

		// Deserialize the lock.
		let lock = serde_json::from_slice::<tg::graph::Data>(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize the lock"),
		)?;

		Ok(Some(lock))
	}

	pub(super) async fn checkin_write_lock(&self, state: &mut State) -> tg::Result<()> {
		// Do not create a lock if this is a destructive checkin or the user did not request one.
		if state.arg.options.destructive || !state.arg.options.lock {
			return Ok(());
		}

		// Create the unstripped lock and strip it to get the mapping.
		let (lock, index_map) = Self::checkin_create_lock_with_mapping(state);

		// Update lock_node fields in the graph nodes using the mapping.
		for (node_id, node) in state.graph.nodes.iter_mut() {
			if let Some(&new_index) = index_map.get(node_id) {
				node.lock_node = Some(new_index);
			} else {
				node.lock_node = None;
			}
		}

		// Update the state's lock to the stripped lock.
		state.lock = Some(lock.clone());

		// If this is a locked checkin, then verify the lock is unchanged.
		if state.arg.options.locked
			&& state
				.lock
				.as_ref()
				.is_some_and(|existing| existing.nodes != lock.nodes)
		{
			return Err(tg::error!("the lock is out of date"));
		}

		// If the root is a directory, then write a lockfile. Otherwise, write a lockattr.
		match state.graph.nodes.get(&0).unwrap().variant {
			Variant::Directory(_) => {
				// Determine the lockfile path.
				let lockfile_path = state.root_path.join(tg::package::LOCKFILE_FILE_NAME);

				// Remove an existing lockfile.
				tangram_util::fs::remove(&lockfile_path).await.ok();

				// Do not write an empty lock.
				if lock.nodes.is_empty() {
					return Ok(());
				}

				// Serialize the lock.
				let contents = serde_json::to_vec_pretty(&lock)
					.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;

				// Write the lockfile.
				tokio::fs::write(&lockfile_path, contents)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the lock"))?;
			},

			Variant::File(_) => {
				// Remove an existing lockattr.
				xattr::remove(&state.root_path, tg::file::LOCKATTR_XATTR_NAME).ok();

				// Do not write an empty lock.
				if lock.nodes.is_empty() {
					return Ok(());
				}

				// Serialize the lock.
				let contents = serde_json::to_vec(&lock)
					.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;

				// Write the lockattr.
				xattr::set(&state.root_path, tg::file::LOCKATTR_XATTR_NAME, &contents)
					.map_err(|source| tg::error!(!source, "failed to write the lockatttr"))?;
			},

			Variant::Symlink(_) => {},
		}

		Ok(())
	}

	fn checkin_create_lock_with_mapping(
		state: &State,
	) -> (tg::graph::Data, BTreeMap<usize, usize>) {
		// Create the nodes, sorted by usize to ensure deterministic ordering.
		let mut sorted_nodes: Vec<_> = state.graph.nodes.iter().collect();
		sorted_nodes.sort_by_key(|(node_id, _)| *node_id);

		let mut nodes = Vec::with_capacity(state.graph.nodes.len());
		let mut ids = Vec::with_capacity(state.graph.nodes.len());
		let mut node_id_to_index = BTreeMap::new();
		for (node_id, node) in sorted_nodes {
			let index = nodes.len();
			node_id_to_index.insert(*node_id, index);
			ids.push(node.object_id.clone().unwrap());
			let lock_node = match &node.variant {
				Variant::Directory(directory) => {
					let mut entries = BTreeMap::new();
					for (name, edge) in &directory.entries {
						entries.insert(name.clone(), edge.clone());
					}
					let data = tg::graph::data::Directory { entries };
					tg::graph::data::Node::Directory(data)
				},

				Variant::File(file) => {
					let mut dependencies = BTreeMap::new();
					for (reference, referent) in &file.dependencies {
						dependencies.insert(reference.clone(), referent.clone());
					}
					let data = tg::graph::data::File {
						contents: None,
						dependencies,
						executable: false,
					};
					tg::graph::data::Node::File(data)
				},

				Variant::Symlink(symlink) => {
					let data = tg::graph::data::Symlink {
						artifact: symlink.artifact.clone(),
						path: None,
					};
					tg::graph::data::Node::Symlink(data)
				},
			};
			nodes.push(lock_node);
		}

		// Create the lock.
		let lock = tg::graph::Data { nodes };

		// Strip the lock and get the index mapping.
		let (stripped_lock, index_to_index_map) = Self::strip_lock(lock, &ids);

		// Convert the mapping from index-to-index to NodeId-to-index.
		let mut node_id_to_stripped_index = BTreeMap::new();
		for (node_id, original_index) in node_id_to_index {
			if let Some(&stripped_index) = index_to_index_map.get(&original_index) {
				node_id_to_stripped_index.insert(node_id, stripped_index);
			}
		}

		(stripped_lock, node_id_to_stripped_index)
	}

	#[allow(dead_code)]
	fn checkin_create_lock(state: &State) -> tg::graph::Data {
		let (lock, _map) = Self::checkin_create_lock_with_mapping(state);
		lock
	}

	pub(crate) fn strip_lock(
		lock: tg::graph::Data,
		ids: &[tg::object::Id],
	) -> (tg::graph::Data, BTreeMap<usize, usize>) {
		// Run Tarjan's algorithm.
		let sccs = petgraph::algo::tarjan_scc(&Petgraph(&lock));

		// Mark nodes that refer to tagged items.
		let mut marks = vec![false; lock.nodes.len()];
		for scc in &sccs {
			let marked = scc.iter().copied().any(|index| {
				marks[index]
					|| match &lock.nodes[index] {
						tg::graph::data::Node::Directory(directory) => directory
							.entries
							.values()
							.filter_map(|edge| edge.try_unwrap_reference_ref().ok())
							.any(|reference| marks[reference.node]),
						tg::graph::data::Node::File(file) => file
							.dependencies
							.iter()
							.filter_map(|(reference, referent)| {
								let item =
									referent.as_ref()?.item().try_unwrap_reference_ref().ok()?;
								Some((reference, item))
							})
							.any(|(reference, item)| {
								marks[item.node]
									|| (reference.options().local.is_none()
										&& reference.item().is_tag())
							}),
						tg::graph::data::Node::Symlink(symlink) => symlink
							.artifact
							.as_ref()
							.and_then(|edge| edge.try_unwrap_reference_ref().ok())
							.is_some_and(|reference| marks[reference.node]),
					}
			});
			if marked {
				for index in scc.iter().copied() {
					marks[index] = true;
				}
			}
		}

		// Create the nodes and map.
		let mut nodes = Vec::new();
		let mut map = BTreeMap::new();
		for (index, (mark, node)) in std::iter::zip(&marks, lock.nodes).enumerate() {
			if *mark {
				map.insert(index, nodes.len());
				nodes.push(node);
			}
		}

		// Update indexes and remove unmarked edges.
		for node in &mut nodes {
			match node {
				tg::graph::data::Node::Directory(directory) => {
					directory.entries.retain(|_name, edge| match edge {
						tg::graph::data::Edge::Reference(reference) => marks[reference.node],
						tg::graph::data::Edge::Object(_) => true,
					});
					for edge in directory.entries.values_mut() {
						if let tg::graph::data::Edge::Reference(reference) = edge {
							reference.node = map.get(&reference.node).copied().unwrap();
						}
					}
				},
				tg::graph::data::Node::File(file) => {
					file.dependencies.retain(|reference, referent| {
						let Some(node) = referent
							.as_ref()
							.and_then(|r| Some(r.item().try_unwrap_reference_ref().ok()?.node))
						else {
							return true;
						};
						marks[node]
							|| (reference.options().local.is_none() && reference.item().is_tag())
					});
					for referent in file.dependencies.values_mut() {
						let Some(referent) = referent else {
							continue;
						};
						let edge = &mut referent.item;
						match edge {
							tg::graph::data::Edge::Reference(reference)
								if marks[reference.node] =>
							{
								reference.node = map.get(&reference.node).copied().unwrap();
							},
							tg::graph::data::Edge::Reference(reference) => {
								let id = ids[reference.node].clone();
								referent.item = tg::graph::data::Edge::Object(id);
							},
							tg::graph::data::Edge::Object(_) => (),
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(tg::graph::data::Edge::Reference(reference)) = &mut symlink.artifact
					{
						reference.node = map.get(&reference.node).copied().unwrap();
					}
				},
			}
		}

		(tg::graph::Data { nodes }, map)
	}
}

struct Petgraph<'a>(&'a tg::graph::Data);

impl petgraph::visit::GraphBase for Petgraph<'_> {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl petgraph::visit::IntoNodeIdentifiers for &Petgraph<'_> {
	type NodeIdentifiers = std::ops::Range<usize>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.nodes.len()
	}
}

impl petgraph::visit::NodeIndexable for Petgraph<'_> {
	fn node_bound(&self) -> usize {
		self.0.nodes.len()
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}

	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &Petgraph<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		match &self.0.nodes[id] {
			tg::graph::data::Node::Directory(directory) => directory
				.entries
				.values()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node)
				})
				.boxed(),
			tg::graph::data::Node::File(file) => file
				.dependencies
				.values()
				.filter_map(|referent| {
					referent
						.as_ref()?
						.item
						.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node)
				})
				.boxed(),
			tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, .. }) => artifact
				.iter()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node)
				})
				.boxed(),
		}
	}
}
