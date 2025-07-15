use crate::Server;
use tangram_client as tg;
use tangram_either::Either;
use tangram_itertools::IteratorExt as _;

impl Server {
	pub(super) fn create_lockfile(state: &super::State) -> tg::Result<tg::Lockfile> {
		// Create lockfile nodes for every node of the graph.
		let mut nodes = Vec::with_capacity(state.graph.nodes.len());
		let mut objects = vec![None; state.graph.nodes.len()];
		let mut visited = vec![None; state.graph.nodes.len()];
		let root = Self::create_lockfile_node(state, 0, &mut nodes, &mut objects, &mut visited)?
			.unwrap_left();

		// Collect the full lockfile graph.
		let nodes = nodes
			.into_iter()
			.map(|node| node.unwrap())
			.collect::<Vec<_>>();

		// Strip the lockfile nodes.
		let nodes = Self::strip_lockfile_nodes(&nodes, &objects, root)?;

		Ok(tg::Lockfile { nodes })
	}

	pub(super) fn create_lockfile_node(
		state: &super::State,
		node: usize,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<Either<usize, tg::object::Id>> {
		// Make sure parents are visited first.
		if let Some(parent) = state.graph.nodes[node].parent {
			Self::create_lockfile_node(state, parent, nodes, objects, visited)?;
		}

		// Check if this node is visited.
		if let Some(visited) = visited[node].clone() {
			return Ok(visited);
		}

		// If the object is not an artifact, return its ID.
		let id = state.graph.nodes[node].object.as_ref().unwrap().id.clone();
		if !(id.is_directory() || id.is_file() || id.is_symlink()) {
			visited[node].replace(Either::Right(id.clone()));
			return Ok(Either::Right(id));
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
			return Ok(Either::Right(id));
		};

		// Otherwise createa a slot for it.
		let index = nodes.len();
		visited[node].replace(Either::Left(index));
		nodes.push(None);

		// Update the object ID.
		objects[index] = state.graph.nodes[node]
			.path
			.as_deref()
			.is_none_or(|path| path.strip_prefix(&state.artifacts_path).is_ok())
			.then(|| state.graph.nodes[node].object.as_ref().unwrap().id.clone());

		// Create the node.
		let data = data.try_into().unwrap();
		let lockfile_node = match &data {
			tg::artifact::Data::Directory(data) => Self::checkin_create_lockfile_directory_node(
				state, node, data, nodes, objects, visited,
			)?,
			tg::artifact::Data::File(data) => {
				Self::checkin_create_lockfile_file_node(state, node, data, nodes, objects, visited)?
			},
			tg::artifact::Data::Symlink(data) => Self::checkin_create_lockfile_symlink_node(
				state, node, data, nodes, objects, visited,
			)?,
		};

		nodes[index].replace(lockfile_node);
		Ok(Either::Left(index))
	}

	pub(super) fn checkin_create_lockfile_directory_node(
		state: &super::State,
		node: usize,
		_data: &tg::directory::Data,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		let entries = state.graph.nodes[node]
			.variant
			.unwrap_directory_ref()
			.entries
			.iter()
			.map(|(name, node)| {
				let entry = Self::create_lockfile_node(state, *node, nodes, objects, visited)?;
				Ok::<_, tg::Error>((name.clone(), entry))
			})
			.collect::<tg::Result<_>>()?;
		let directory = tg::lockfile::Directory { entries };
		Ok(tg::lockfile::Node::Directory(directory))
	}

	pub(super) fn checkin_create_lockfile_file_node(
		state: &super::State,
		index: usize,
		data: &tg::file::Data,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
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
						let item = Either::Right(id);
						Ok((reference, referent.map(|_| item)))
					},
					Either::Right(node) => {
						let item =
							Self::create_lockfile_node(state, node, nodes, objects, visited)?;
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
		let file = tg::lockfile::File {
			contents: Some(contents),
			dependencies,
			executable,
		};
		Ok(tg::lockfile::Node::File(file))
	}

	#[allow(clippy::ptr_arg)]
	fn checkin_create_lockfile_symlink_node(
		state: &super::State,
		node: usize,
		data: &tg::symlink::Data,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		// Get the artifact, if it exists.
		let variant = &state.graph.nodes[node].variant.unwrap_symlink_ref();
		let artifact = variant
			.artifact
			.as_ref()
			.map(|artifact| match artifact {
				Either::Left(id) => Ok(Either::Right(id.clone().into())),
				Either::Right(index) => {
					Self::create_lockfile_node(state, *index, nodes, objects, visited)
				},
			})
			.transpose()?;

		// Get the subpath, if it exists.
		let subpath = match data {
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

		// Create the lockfile node.
		Ok(tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
			artifact,
			path: subpath,
		}))
	}

	pub fn strip_lockfile_nodes(
		old_nodes: &[tg::lockfile::Node],
		object_ids: &[Option<tg::object::Id>],
		root: usize,
	) -> tg::Result<Vec<tg::lockfile::Node>> {
		let preserve = mark_nodes_to_preserve(old_nodes);

		// Strip nodes that don't reference tag dependencies.
		let mut new_nodes = Vec::with_capacity(old_nodes.len());
		let mut visited = vec![None; old_nodes.len()];
		strip_nodes_inner(
			old_nodes,
			root,
			&mut visited,
			&mut new_nodes,
			object_ids,
			&preserve,
		);

		// Construct a new lockfile with only stripped nodes.
		Ok(new_nodes.into_iter().map(Option::unwrap).collect())
	}
}

// Marks nodes if they or any of their transitive children are eligible for deletion.
fn mark_nodes_to_preserve(nodes: &[tg::lockfile::Node]) -> Vec<bool> {
	// Add the child to the same set as the parent.
	fn union(set: &mut [usize], parent: usize, child: usize) {
		set[child] = find(set, parent);
	}

	// Find the characteristic set of an item.
	fn find(set: &mut [usize], item: usize) -> usize {
		let mut current = item;
		loop {
			if set[current] == current {
				set[item] = current;
				return current;
			}
			current = set[current];
		}
	}
	// Split into sccs.
	let sccs = petgraph::algo::tarjan_scc(&LockfileGraphImpl(nodes));

	// Mark tagged items.
	let mut set = (0..=nodes.len()).collect::<Vec<_>>();
	let tagged = nodes.len();

	// First mark nodes to tag. This is done in a separate pass because it operates top-down.
	for scc in &sccs {
		for parent in scc.iter().copied() {
			match &nodes[parent] {
				tg::lockfile::Node::Directory(directory) => {
					for child in directory
						.entries
						.values()
						.filter_map(|entry| entry.as_ref().left().copied())
					{
						// Mark directory children as part of the same set as their parent.
						union(&mut set, parent, child);
					}
				},
				tg::lockfile::Node::File(file) => {
					for (child, is_tagged) in
						file.dependencies
							.iter()
							.filter_map(|(reference, referent)| {
								let child = referent.item.as_ref().left().copied()?;
								let is_tagged = reference.item().try_unwrap_tag_ref().is_ok()
									&& reference.path().is_none();
								Some((child, is_tagged))
							}) {
						if is_tagged {
							// If a dependency is tagged, add it to the set of tagged nodes.
							union(&mut set, tagged, child);
						} else {
							// Otherwise, mark it as part of the same set as the parent.
							union(&mut set, parent, child);
						}
					}
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
					artifact: Some(Either::Left(child)),
					..
				}) => {
					union(&mut set, parent, *child);
				},
				tg::lockfile::Node::Symlink(_) => (),
			}
		}
	}

	// Now, determine which nodes to preserve.
	let mut preserve = vec![false; nodes.len()];
	for scc in sccs {
		// Next, check if any dependencies are marked.
		for node in scc.iter().copied() {
			let has_dependencies = match &nodes[node] {
				tg::lockfile::Node::Directory(directory) => directory
					.entries
					.values()
					.filter_map(|entry| entry.as_ref().left().copied())
					.any(|node| preserve[node] || find(&mut set, node) == tagged),
				tg::lockfile::Node::File(file) => {
					file.dependencies.iter().any(|(reference, referent)| {
						(reference.item().try_unwrap_tag_ref().is_ok()
							&& reference.path().is_none())
							|| referent.item.as_ref().left().is_some_and(|item| {
								preserve[*item] || find(&mut set, node) == tagged
							})
					})
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
					artifact: Some(Either::Left(node)),
					..
				}) => preserve[*node] || find(&mut set, *node) == tagged,
				tg::lockfile::Node::Symlink(_) => false,
			};
			preserve[node] = has_dependencies;
		}
	}

	preserve
}

fn strip_nodes_inner(
	old_nodes: &[tg::lockfile::Node], // the full lockfile graph.
	node: usize,                      // the current node we're visiting
	visited: &mut Vec<Option<usize>>, // visited set for cycle handling
	new_nodes: &mut Vec<Option<tg::lockfile::Node>>, // the list of nodes we're adding to
	object_ids: &[Option<tg::object::Id>], // a list of each node's object ID, None if they are a path dependency.
	preserve: &[bool],                     // a list of whether nodes should be preserved.
) -> Option<Either<usize, tg::object::Id>> {
	if !preserve[node] {
		return object_ids[node].clone().map(Either::Right);
	}

	if let Some(visited) = visited[node] {
		return Some(Either::Left(visited));
	}

	let new_node = new_nodes.len();
	visited[node].replace(new_node);
	new_nodes.push(None);

	match old_nodes[node].clone() {
		tg::lockfile::Node::Directory(directory) => {
			let entries = directory
				.entries
				.into_iter()
				.filter_map(|(name, entry)| {
					let entry = match entry {
						Either::Left(node) => strip_nodes_inner(
							old_nodes, node, visited, new_nodes, object_ids, preserve,
						),
						Either::Right(id) => Some(Either::Right(id)),
					};
					Some((name, entry?))
				})
				.collect();

			// Create a new node.
			let directory = tg::lockfile::Directory { entries };
			new_nodes[new_node].replace(tg::lockfile::Node::Directory(directory));
		},
		tg::lockfile::Node::File(file) => {
			let dependencies = file
				.dependencies
				.into_iter()
				.filter_map(|(reference, referent)| {
					// Special case, the reference is by ID.
					let item: Either<usize, tg::object::Id> = match referent.item() {
						Either::Left(node) => {
							if let Some(node) = strip_nodes_inner(
								old_nodes, *node, visited, new_nodes, object_ids, preserve,
							) {
								node
							} else if let Ok(id) = reference.item().try_unwrap_object_ref() {
								Either::Right(id.clone())
							} else {
								return None;
							}
						},
						Either::Right(id) => Either::Right(id.clone()),
					};
					Some((reference, referent.map(|_| item)))
				})
				.collect();

			// Retain the contents if this is not a path dependency.
			let contents = if object_ids[node].is_none() {
				None
			} else {
				file.contents
			};

			// Create the node.
			let file = tg::lockfile::File {
				contents,
				dependencies,
				executable: file.executable,
			};
			new_nodes[new_node].replace(tg::lockfile::Node::File(file));
		},

		tg::lockfile::Node::Symlink(tg::lockfile::Symlink { artifact, path }) => {
			// Remap the artifact if necessary.
			let artifact = match artifact {
				Some(Either::Left(node)) => {
					strip_nodes_inner(old_nodes, node, visited, new_nodes, object_ids, preserve)
				},
				Some(Either::Right(id)) => Some(Either::Right(id)),
				None => None,
			};

			// Create the node.
			new_nodes[new_node].replace(tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
				artifact,
				path,
			}));
		},
	}

	Some(Either::Left(new_node))
}

struct LockfileGraphImpl<'a>(&'a [tg::lockfile::Node]);

impl petgraph::visit::GraphBase for LockfileGraphImpl<'_> {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl petgraph::visit::NodeIndexable for &LockfileGraphImpl<'_> {
	fn from_index(&self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(&self) -> usize {
		self.0.len()
	}

	fn to_index(&self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &LockfileGraphImpl<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		match &self.0[a] {
			tg::lockfile::Node::Directory(directory) => directory
				.entries
				.values()
				.filter_map(|entry| entry.as_ref().left().copied())
				.boxed(),
			tg::lockfile::Node::File(file) => file
				.dependencies
				.values()
				.filter_map(|referent| referent.item.as_ref().left().copied())
				.boxed(),
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink { artifact, .. }) => artifact
				.clone()
				.into_iter()
				.filter_map(Either::left)
				.boxed(),
		}
	}
}

impl petgraph::visit::IntoNodeIdentifiers for &LockfileGraphImpl<'_> {
	type NodeIdentifiers = std::ops::Range<usize>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.len()
	}
}

#[cfg(test)]
mod tests {
	use indoc::indoc;
	use tangram_client as tg;

	#[test]
	fn strip() {
		let lockfile = serde_json::from_str::<tg::Lockfile>(indoc!(
			r#"
				{
				  "nodes": [
				    {
				      "kind": "directory",
				      "entries": {
				        "tangram.ts": 1,
				        "foo.tg.ts": 2
				      }
				    },
				    {
				      "kind": "file",
				      "dependencies": {
				        "a": {
				          "item": 3,
				          "tag": "a"
				        },
				        "./foo.tg.ts": {
				          "item": 2
				        }
				      }
				    },
				    {
				      "kind": "file"
				    },
				    {
				      "kind": "directory",
				      "entries": {
				        "tangram.ts": 4
				      }
				    },
				    {
				      "kind": "file",
				      "contents": "blb_01038pab1jh9r3ztm2811kzr14ff3223xhcp9dgczg1gd1afmje6ng"
				    }
				  ]
				}
			"#
		))
		.unwrap();

		// Test that marking the nodes works.
		let preserve = super::mark_nodes_to_preserve(&lockfile.nodes);
		assert_eq!(&preserve, &[true, true, false, true, false]);
	}
}
