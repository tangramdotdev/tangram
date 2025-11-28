use {
	super::graph::Variant,
	crate::{Server, checkin::Graph},
	std::{
		collections::{BTreeMap, HashSet, VecDeque},
		path::Path,
	},
	tangram_client::prelude::*,
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
						tg::error!(!source, path = %lock_path.display(), "failed to read the lockfile"),
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
			|source| tg::error!(!source, path = %path.display(), "failed to deserialize the lock"),
		)?;

		Ok(Some(lock))
	}

	pub(super) async fn checkin_write_lock(
		&self,
		arg: &tg::checkin::Arg,
		graph: &Graph,
		next: usize,
		lock: Option<&tg::graph::Data>,
		root: &Path,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<()> {
		progress.spinner("locking", "locking");

		// Get the root node.
		let root_index = graph.paths.get(root).unwrap();
		let root_node = graph.nodes.get(root_index).unwrap();

		// Do not write a lock if the lock flag is false.
		if !arg.options.lock {
			return Ok(());
		}

		// Do not write a lock if this is a destructive checkin.
		if arg.options.destructive {
			return Ok(());
		}

		// If the root is not solvable, then remove an existing lock and return.
		if !root_node.solvable {
			match root_node.variant {
				Variant::Directory(_) => {
					let lockfile_path = root.join(tg::package::LOCKFILE_FILE_NAME);
					tangram_util::fs::remove(&lockfile_path).await.ok();
				},
				Variant::File(_) => {
					xattr::remove(root, tg::file::LOCKATTR_XATTR_NAME).ok();
				},
				Variant::Symlink(_) => (),
			}
			return Ok(());
		}

		// Do not write a lock if the lock was not changed during solving.
		if let Some(lock) = lock {
			let changed = Self::checkin_lock_changed(graph, next, lock);
			if !changed {
				return Ok(());
			}
		}

		// Create the lock.
		let new_lock = Self::checkin_create_lock(graph, root);

		// If this is a locked checkin, then verify the lock is unchanged.
		if arg.options.locked && lock.is_some_and(|existing| new_lock.nodes != existing.nodes) {
			return Err(tg::error!("the lock is out of date"));
		}

		let lock = new_lock;

		// If the root is a directory, then write a lockfile. Otherwise, write a lockattr.
		match root_node.variant {
			Variant::Directory(_) => {
				// Determine the lockfile path.
				let lockfile_path = root.join(tg::package::LOCKFILE_FILE_NAME);

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
				xattr::remove(root, tg::file::LOCKATTR_XATTR_NAME).ok();

				// Do not write an empty lock.
				if lock.nodes.is_empty() {
					return Ok(());
				}

				// Serialize the lock.
				let contents = serde_json::to_vec(&lock)
					.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;

				// Write the lockattr.
				xattr::set(root, tg::file::LOCKATTR_XATTR_NAME, &contents)
					.map_err(|source| tg::error!(!source, "failed to write the lockatttr"))?;
			},

			Variant::Symlink(_) => {},
		}

		progress.finish("locking");

		Ok(())
	}

	fn checkin_lock_changed(
		graph: &Graph,
		next: usize,
		lock: &tangram_client::graph::Data,
	) -> bool {
		graph.nodes.range(next..).any(|(_, node)| {
			if let Some(lock_node) = node.lock_node {
				let lock_node = lock.nodes.get(lock_node).unwrap();
				match (lock_node, &node.variant) {
					// If a directory removed an entry that was present in the lock, then the lock changed.
					(tg::graph::data::Node::Directory(lock), Variant::Directory(node)) => {
						for name in lock.entries.keys() {
							if !node.entries.contains_key(name) {
								return true;
							}
						}
					},
					(tg::graph::data::Node::File(lock), Variant::File(node)) => {
						if node.dependencies.iter().any(|(reference, referent)| {
							if !reference.is_solvable() {
								return false;
							}
							if let Some(lock) = lock.dependencies.get(reference) {
								// If a file had a dependency change, then the lock changed.
								if lock.as_ref().map(|lock| &lock.options)
									!= referent.as_ref().map(|referent| &referent.options)
								{
									return true;
								}
							} else {
								// If a file added a solvable dependency, then the lock changed.
								return true;
							}
							false
						}) {
							return true;
						}

						// If a file removed a dependency that was present in the lock, then the lock changed.
						for reference in lock.dependencies.keys() {
							if !node.dependencies.contains_key(reference) {
								return true;
							}
						}
					},
					(tg::graph::data::Node::Symlink(_), Variant::Symlink(_)) => (),
					_ => {
						return true;
					},
				}
			} else if let Variant::File(file) = &node.variant {
				// If a file with solvable dependencies was added, then the lock changed.
				if file
					.dependencies
					.iter()
					.any(|(reference, _)| reference.is_solvable())
				{
					return true;
				}
			}
			false
		})
	}

	fn checkin_create_lock(graph: &Graph, root: &Path) -> tg::graph::Data {
		// Get the root node index.
		let root_index = *graph.paths.get(root).unwrap();

		let mut nodes = Vec::with_capacity(graph.nodes.len());
		let mut ids = Vec::with_capacity(graph.nodes.len());
		let mut visited = HashSet::new();
		let mut queue = VecDeque::new();
		let mut mapping = BTreeMap::new();

		queue.push_back(root_index);

		while let Some(index) = queue.pop_front() {
			// Skip if already visited.
			if !visited.insert(index) {
				continue;
			}

			// Record the mapping from graph index to lock index.
			mapping.insert(index, nodes.len());

			// Get the node.
			let node = &graph.nodes.get(&index).unwrap();
			ids.push(node.id.clone().unwrap());

			// Create the lock node.
			let (lock_node, children) = match &node.variant {
				Variant::Directory(directory) => {
					let mut entries = BTreeMap::new();
					let mut children = Vec::new();
					for (name, edge) in &directory.entries {
						entries.insert(name.clone(), edge.clone());
						if let Ok(reference) = edge.try_unwrap_reference_ref()
							&& reference.graph.is_none()
							&& !visited.contains(&reference.index)
						{
							children.push(reference.index);
						}
					}
					let data = tg::graph::data::Directory { entries };
					(tg::graph::data::Node::Directory(data), children)
				},

				Variant::File(file) => {
					let mut dependencies = BTreeMap::new();
					let mut children = Vec::new();
					for (reference, referent) in &file.dependencies {
						dependencies.insert(reference.clone(), referent.clone());
						if let Some(referent_value) = referent
							&& let Ok(edge_reference) =
								referent_value.item.try_unwrap_reference_ref()
							&& edge_reference.graph.is_none()
							&& !visited.contains(&edge_reference.index)
						{
							children.push(edge_reference.index);
						}
					}
					let data = tg::graph::data::File {
						contents: None,
						dependencies,
						executable: false,
					};
					(tg::graph::data::Node::File(data), children)
				},

				Variant::Symlink(symlink) => {
					let mut children = Vec::new();
					if let Some(edge) = &symlink.artifact
						&& let Ok(reference) = edge.try_unwrap_reference_ref()
						&& reference.graph.is_none()
						&& !visited.contains(&reference.index)
					{
						children.push(reference.index);
					}
					let data = tg::graph::data::Symlink {
						artifact: symlink.artifact.clone(),
						path: None,
					};
					(tg::graph::data::Node::Symlink(data), children)
				},
			};

			// Add the node to the lock.
			nodes.push(lock_node);

			// Add the children to the queue.
			for child in children {
				queue.push_back(child);
			}
		}

		// Remap from graph indices to lock indices.
		for node in &mut nodes {
			match node {
				tg::graph::data::Node::Directory(directory) => {
					for edge in directory.entries.values_mut() {
						if let tg::graph::data::Edge::Reference(reference) = edge
							&& reference.graph.is_none()
							&& let Some(lock_index) = mapping.get(&reference.index)
						{
							reference.index = *lock_index;
						}
					}
				},
				tg::graph::data::Node::File(file) => {
					for referent_value in file.dependencies.values_mut().flatten() {
						if let tg::graph::data::Edge::Reference(reference) =
							&mut referent_value.item
							&& reference.graph.is_none()
							&& let Some(lock_index) = mapping.get(&reference.index)
						{
							reference.index = *lock_index;
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(tg::graph::data::Edge::Reference(reference)) = &mut symlink.artifact
						&& reference.graph.is_none()
						&& let Some(lock_index) = mapping.get(&reference.index)
					{
						reference.index = *lock_index;
					}
				},
			}
		}

		// Create the lock.
		let lock = tg::graph::Data { nodes };

		// Strip the lock.
		Self::strip_lock(lock, &ids)
	}

	pub(crate) fn strip_lock(lock: tg::graph::Data, ids: &[tg::object::Id]) -> tg::graph::Data {
		// Run Tarjan's algorithm.
		let sccs = petgraph::algo::tarjan_scc(&Petgraph(&lock));

		// Mark.
		let mut marks = vec![false; lock.nodes.len()];
		for scc in &sccs {
			let marked = scc.iter().copied().any(|index| {
				marks[index]
					|| match &lock.nodes[index] {
						tg::graph::data::Node::Directory(directory) => directory
							.entries
							.values()
							.filter_map(|edge| edge.try_unwrap_reference_ref().ok())
							.any(|reference| marks[reference.index]),
						tg::graph::data::Node::File(file) => {
							file.dependencies.iter().any(|(reference, referent)| {
								let Some(referent) = referent else {
									return false;
								};
								let Ok(item) = referent.item().try_unwrap_reference_ref() else {
									return false;
								};
								marks[item.index]
									|| (reference.is_solvable()
										&& (referent.id().is_some() || referent.tag().is_some()))
							})
						},
						tg::graph::data::Node::Symlink(symlink) => symlink
							.artifact
							.as_ref()
							.and_then(|edge| edge.try_unwrap_reference_ref().ok())
							.is_some_and(|reference| marks[reference.index]),
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
					// Remove unmarked entries.
					directory.entries.retain(|_name, edge| match edge {
						tg::graph::data::Edge::Reference(reference) => marks[reference.index],
						tg::graph::data::Edge::Object(_) => true,
					});
					for edge in directory.entries.values_mut() {
						if let tg::graph::data::Edge::Reference(reference) = edge {
							reference.index = map.get(&reference.index).copied().unwrap();
						}
					}
				},
				tg::graph::data::Node::File(file) => {
					// Remove unmarked dependencies.
					file.dependencies.retain(|reference, referent| {
						let Some(referent) = referent else {
							return false;
						};
						let Ok(item) = referent.item().try_unwrap_reference_ref() else {
							return false;
						};
						marks[item.index]
							|| (reference.is_solvable()
								&& (referent.id().is_some() || referent.tag().is_some()))
					});

					// Update indexes.
					for referent in file.dependencies.values_mut() {
						let Some(referent) = referent else {
							continue;
						};
						let tg::graph::data::Edge::Reference(reference) = &mut referent.item else {
							continue;
						};
						if marks[reference.index] {
							reference.index = map.get(&reference.index).copied().unwrap();
						} else {
							let id = ids[reference.index].clone();
							referent.item = tg::graph::data::Edge::Object(id);
						}
					}
				},
				tg::graph::data::Node::Symlink(symlink) => {
					if let Some(tg::graph::data::Edge::Reference(reference)) = &mut symlink.artifact
					{
						reference.index = map.get(&reference.index).copied().unwrap();
					}
				},
			}
		}

		tg::graph::Data { nodes }
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
						.map(|reference| reference.index)
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
						.map(|reference| reference.index)
				})
				.boxed(),
			tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, .. }) => artifact
				.iter()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.index)
				})
				.boxed(),
		}
	}
}
