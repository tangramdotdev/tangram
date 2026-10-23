use {
	std::{collections::BTreeMap, path::Path},
	tangram_client::prelude::*,
	tangram_util::iter::Ext as _,
};

struct Petgraph<'a>(&'a tg::graph::data::Leaf);

pub fn try_read_lock(path: &Path) -> tg::Result<Option<tg::graph::data::Leaf>> {
	// Try to read the lock contents.
	let contents = if path.is_dir() {
		let lockfile_path = path.join(tg::module::LOCKFILE_FILE_NAME);
		try_read_lockfile(&lockfile_path)?
	} else if std::fs::metadata(path).is_ok_and(|metadata| metadata.is_file()) {
		let lockfile_path = path.with_extension("lock");
		let contents = try_read_lockfile(&lockfile_path)?;
		if contents.is_some() {
			contents
		} else {
			// Fall back to xattr.
			tg::file::xattrs::read_lock(path).ok().flatten()
		}
	} else {
		None
	};

	// Return early if no contents found.
	let Some(contents) = contents else {
		return Ok(None);
	};

	// Deserialize the lock.
	let lock = serde_json::from_slice::<tg::graph::data::Leaf>(&contents).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to deserialize the lock"),
	)?;

	Ok(Some(lock))
}

#[must_use]
pub fn strip_lock(lock: tg::graph::data::Leaf, source_dependencies: bool) -> tg::graph::data::Leaf {
	// Run Tarjan's algorithm.
	let sccs = petgraph::algo::tarjan_scc(&Petgraph(&lock));

	// Mark.
	let mut marks = vec![false; lock.nodes.len()];
	for scc in &sccs {
		let marked = scc.iter().copied().any(|index| {
			marks[index]
				|| match &lock.nodes[index] {
					tg::graph::data::Node::Directory(directory) => {
						if let tg::graph::data::Directory::Leaf(leaf) = directory {
							leaf.entries
								.values()
								.filter_map(|edge: &tg::graph::data::Edge<tg::artifact::Id>| {
									edge.try_unwrap_index_ref().ok()
								})
								.any(|index| marks[*index])
						} else {
							false
						}
					},
					tg::graph::data::Node::File(file) => {
						file.dependencies.iter().any(|(reference, option)| {
							if source_dependencies && reference.options().source.is_some() {
								return false;
							}
							let Some(dependency) = option else {
								return false;
							};
							let Some(edge) = dependency.node() else {
								return false;
							};
							let solvable = reference.is_solvable()
								&& (dependency.id().is_some() || dependency.tag().is_some());
							match edge {
								tg::graph::data::Edge::Index(index) => marks[*index] || solvable,
								tg::graph::data::Edge::Pointer(_) => solvable,
								tg::graph::data::Edge::Object(_) => false,
							}
						})
					},
					tg::graph::data::Node::Symlink(symlink) => symlink
						.artifact
						.as_ref()
						.and_then(|edge| edge.try_unwrap_index_ref().ok())
						.is_some_and(|index| marks[*index]),
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
				if let tg::graph::data::Directory::Leaf(leaf) = directory {
					// Remove unmarked entries.
					leaf.entries.retain(|_name, edge| match edge {
						tg::graph::data::Edge::Index(index) => marks[*index],
						tg::graph::data::Edge::Pointer(_) | tg::graph::data::Edge::Object(_) => {
							true
						},
					});

					for edge in leaf.entries.values_mut() {
						if let tg::graph::data::Edge::Index(target_index) = edge {
							*target_index = map.get(&*target_index).copied().unwrap();
						}
					}
				}
			},
			tg::graph::data::Node::File(file) => {
				// Remove unmarked dependencies.
				file.dependencies.retain(|reference, option| {
					if source_dependencies && reference.options().source.is_some() {
						return false;
					}
					let Some(dependency) = option else {
						return false;
					};
					let Some(edge) = dependency.node() else {
						return false;
					};
					let solvable = reference.is_solvable()
						&& (dependency.id().is_some() || dependency.tag().is_some());
					match edge {
						tg::graph::data::Edge::Index(index) => marks[*index] || solvable,
						tg::graph::data::Edge::Pointer(_) => solvable,
						tg::graph::data::Edge::Object(_) => false,
					}
				});

				// Update indexes.
				for dependency in file.dependencies.values_mut() {
					let Some(dependency) = dependency else {
						continue;
					};
					let Some(tg::graph::data::Edge::Index(target_index)) = &mut dependency.node
					else {
						continue;
					};

					if marks[*target_index] {
						*target_index = map.get(&*target_index).copied().unwrap();
					} else {
						dependency.node = None;
					}
				}
			},
			tg::graph::data::Node::Symlink(symlink) => {
				if let Some(tg::graph::data::Edge::Index(target_index)) = &mut symlink.artifact {
					*target_index = map.get(&*target_index).copied().unwrap();
				}
			},
		}
	}

	tg::graph::data::Leaf { nodes }
}

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
			tg::graph::data::Node::Directory(directory) => {
				if let tg::graph::data::Directory::Leaf(leaf) = directory {
					leaf.entries
						.values()
						.filter_map(|edge: &tg::graph::data::Edge<tg::artifact::Id>| {
							let target_index = edge.try_unwrap_index_ref().ok()?;
							// Return the internal node index.
							Some(*target_index)
						})
						.boxed()
				} else {
					std::iter::empty::<usize>().boxed()
				}
			},
			tg::graph::data::Node::File(file) => file
				.dependencies
				.values()
				.filter_map(|option| {
					let target_index = option
						.as_ref()?
						.node
						.as_ref()?
						.try_unwrap_index_ref()
						.ok()?;
					// Return the internal node index.
					Some(*target_index)
				})
				.boxed(),
			tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, .. }) => artifact
				.iter()
				.filter_map(|edge| {
					let target_index = edge.try_unwrap_index_ref().ok()?;
					// Return the internal node index.
					Some(*target_index)
				})
				.boxed(),
		}
	}
}

fn try_read_lockfile(path: &Path) -> tg::Result<Option<Vec<u8>>> {
	match std::fs::read(path) {
		Ok(contents) => Ok(Some(contents)),
		Err(error)
			if matches!(
				error.kind(),
				std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
			) =>
		{
			Ok(None)
		},
		Err(error) => {
			Err(tg::error!(!error, path = %path.display(), "failed to read the lockfile"))
		},
	}
}
