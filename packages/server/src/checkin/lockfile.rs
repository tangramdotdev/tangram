use crate::Server;
use itertools::Itertools;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) fn create_lockfile(state: &super::State) -> tg::Result<tg::Lockfile> {
		// Create lockfile nodes for every node of the graph.
		let mut nodes = Vec::with_capacity(state.graph.nodes.len());
		let mut visited = vec![None; state.graph.nodes.len()];
		let root = Self::create_lockfile_node(state, 0, &mut nodes, &mut visited)?.unwrap_left();

		// Collect the full lockfile graph.
		let nodes = nodes
			.into_iter()
			.map(|node| node.unwrap())
			.collect::<Vec<_>>();

		// Collect a list of which nodes are path dependencies on the local file sysstem.
		let path_dependencies = state
			.graph
			.nodes
			.iter()
			.map(|node| node.path.is_some())
			.collect::<Vec<_>>();

		// Strip the lockfile nodes.
		let nodes = Self::strip_lockfile_nodes(&nodes, &path_dependencies, root)?;

		Ok(tg::Lockfile { nodes })
	}

	pub(super) fn create_lockfile_node(
		state: &super::State,
		node: usize,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<Either<usize, tg::object::Id>> {
		// Make sure parents are visited first.
		if let Some(parent) = state.graph.nodes[node].parent {
			Self::create_lockfile_node(state, parent, nodes, visited)?;
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

		// Otherwise createa a slot for it.
		let index = nodes.len();
		visited[node].replace(Either::Left(index));
		nodes.push(None);

		// Get the artifact data.
		let data: tg::artifact::data::Artifact = state.graph.nodes[node]
			.object
			.as_ref()
			.unwrap()
			.data
			.clone()
			.try_into()
			.unwrap();

		// Create the node.
		let node = match data {
			tg::artifact::Data::Directory(data) => {
				Self::checkin_create_lockfile_directory_node(state, node, &data, nodes, visited)?
			},
			tg::artifact::Data::File(data) => {
				Self::checkin_create_lockfile_file_node(state, node, &data, nodes, visited)?
			},
			tg::artifact::Data::Symlink(data) => {
				Self::checkin_create_lockfile_symlink_node(state, node, &data, nodes, visited)?
			},
		};

		nodes[index].replace(node);
		Ok(Either::Left(index))
	}

	pub(super) fn checkin_create_lockfile_directory_node(
		state: &super::State,
		node: usize,
		_data: &tg::directory::Data,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		let entries = state.graph.nodes[node]
			.variant
			.unwrap_directory_ref()
			.entries
			.iter()
			.map(|(name, node)| {
				let entry = Self::create_lockfile_node(state, *node, nodes, visited)?;
				Ok::<_, tg::Error>((name.clone(), entry))
			})
			.try_collect()?;
		let directory = tg::lockfile::Directory { entries };
		Ok(tg::lockfile::Node::Directory(directory))
	}

	pub(super) fn checkin_create_lockfile_file_node(
		state: &super::State,
		index: usize,
		data: &tg::file::Data,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		let (contents, executable) = match data {
			tg::file::Data::Graph {
				graph: graph_id,
				node,
			} => {
				let graph = &state
					.graph_objects
					.iter()
					.find(|object| object.id == graph_id.clone())
					.unwrap()
					.data;
				let file = graph.nodes[*node].clone().try_unwrap_file().unwrap();
				let contents = file.contents;
				let executable = file.executable;
				(contents, executable)
			},
			tg::file::Data::Normal {
				contents,
				executable,
				..
			} => (contents.clone(), *executable),
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
				match referent.item {
					Either::Left(id) => {
						let item = Either::Right(id);
						let referent = tg::Referent {
							item,
							path: referent.path,
							tag: referent.tag,
						};
						Ok((reference, referent))
					},
					Either::Right(node) => {
						let item = Self::create_lockfile_node(state, node, nodes, visited)?;
						let path = referent
							.path
							.or_else(|| state.graph.referent_path(index, node));
						let tag = referent.tag.or_else(|| state.graph.nodes[node].tag.clone());
						let referent = tg::Referent { item, path, tag };
						Ok::<_, tg::Error>((reference, referent))
					},
				}
			})
			.try_collect()?;
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
		_node: usize,
		data: &tg::symlink::Data,
		_nodes: &mut Vec<Option<tg::lockfile::Node>>,
		_visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		let subpath = match data {
			tg::symlink::Data::Graph {
				graph: graph_id,
				node,
			} => {
				let graph = &state
					.graph_objects
					.iter()
					.find(|object| object.id == graph_id.clone())
					.unwrap()
					.data;
				let symlink = graph.nodes[*node].clone().try_unwrap_symlink().unwrap();
				match symlink {
					tg::graph::data::Symlink::Artifact {
						artifact: _,
						subpath,
					} => subpath,
					tg::graph::data::Symlink::Target { target } => Some(target),
				}
			},
			tg::symlink::Data::Target { target } => Some(PathBuf::from(target)),
			tg::symlink::Data::Artifact {
				artifact: _,
				subpath,
			} => subpath.clone(),
		};

		// The artifact is either another point in the graph, or referred to explicitly by data.
		let artifact = None;

		// Create the lockfile node.
		if let Some(artifact) = artifact {
			Ok(tg::lockfile::Node::Symlink(
				tg::lockfile::Symlink::Artifact { artifact, subpath },
			))
		} else if let Some(subpath) = subpath {
			Ok(tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target {
				target: subpath,
			}))
		} else {
			Err(tg::error!(
				?data,
				"unable to determine subpath for lockfile"
			))
		}
	}

	pub fn strip_lockfile_nodes(
		old_nodes: &[tg::lockfile::Node],
		path_dependencies: &[bool],
		root: usize,
	) -> tg::Result<Vec<tg::lockfile::Node>> {
		let mut strip = vec![true; old_nodes.len()];
		mark_nodes(old_nodes, &mut strip);

		// Strip nodes that don't reference tag dependencies.
		let mut new_nodes = Vec::with_capacity(old_nodes.len());
		let mut visited = vec![None; old_nodes.len()];
		strip_nodes_inner(
			old_nodes,
			root,
			&mut visited,
			&mut new_nodes,
			path_dependencies,
			&strip,
		);

		// Construct a new lockfile with only stripped nodes.
		Ok(new_nodes.into_iter().map(Option::unwrap).collect())
	}
}

// Marks nodes if they or any of their transitive children are eligible for deletion.
fn mark_nodes(nodes: &[tg::lockfile::Node], strip: &mut [bool]) {
	// Union-find implementation. Since this is the only place in the codebase where we need this algorithm it doesn't make sense to split out into a dedicated struct.
	fn union(set: &mut [usize], parent: usize, child: usize) {
		set[child] = find(set, parent);
	}

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

	// Construct the union-find set.
	let mut set = (0..=nodes.len()).collect::<Vec<_>>();

	// Use a sentinel value for the "tagged" set.
	let tagged = nodes.len();

	// Mark tagged items.
	let sccs = petgraph::algo::tarjan_scc(&LockfileGraphImpl(nodes));
	for scc in &sccs {
		for parent in scc.iter().copied() {
			let node = &nodes[parent];
			match node {
				tg::lockfile::Node::Directory(directory) => {
					for child in directory
						.entries
						.values()
						.filter_map(|entry| entry.as_ref().left().copied())
					{
						union(&mut set, parent, child);
					}
				},
				tg::lockfile::Node::File(file) => {
					for (child, is_tagged) in file.dependencies.values().filter_map(|referent| {
						let child = referent.item.as_ref().left().copied()?;
						let is_tagged = referent.tag.is_some();
						Some((child, is_tagged))
					}) {
						if is_tagged {
							union(&mut set, tagged, child);
						} else {
							union(&mut set, parent, child);
						}
					}
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
					artifact: Either::Left(child),
					..
				}) => {
					union(&mut set, parent, *child);
				},
				tg::lockfile::Node::Symlink(_) => (),
			}
		}
	}

	// Now update the marks.
	for scc in &sccs {
		for parent in scc.iter().copied() {
			let node = &nodes[parent];
			if find(&mut set, parent) == tagged {
				strip[parent] = false;
				continue;
			}
			let any_child_tagged = match node {
				tg::lockfile::Node::Directory(directory) => directory
					.entries
					.values()
					.filter_map(|entry| entry.as_ref().left().copied())
					.any(|child| find(&mut set, child) == tagged),
				tg::lockfile::Node::File(file) => file
					.dependencies
					.values()
					.filter_map(|referent| referent.item.as_ref().left().copied())
					.any(|child| find(&mut set, child) == tagged),
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
					artifact: Either::Left(child),
					..
				}) => find(&mut set, *child) == tagged,
				tg::lockfile::Node::Symlink(_) => false,
			};
			if any_child_tagged {
				union(&mut set, tagged, parent);
				strip[parent] = false;
			}
		}
	}
}

fn strip_nodes_inner(
	old_nodes: &[tg::lockfile::Node], // the full lockfile graph.
	node: usize,                      // the current node we're visiting
	visited: &mut Vec<Option<usize>>, // visited set for cycle handling
	new_nodes: &mut Vec<Option<tg::lockfile::Node>>, // the list of nodes we're adding to
	path_dependencies: &[bool],       // a list of which nodes are local path dependencies
	strip: &[bool],                   // a list of whether nodes should be stripped.
) -> Option<usize> {
	// Strip nodes that don't reference modules, are untagged, and not contained within graphs.
	if strip[node] {
		return None;
	}

	if let Some(visited) = visited[node] {
		return Some(visited);
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
							old_nodes,
							node,
							visited,
							new_nodes,
							path_dependencies,
							strip,
						)
						.map(Either::Left),
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
					let tg::Referent { item, path, tag } = referent;

					// Special case, the reference is by ID.
					let item: Either<usize, tg::object::Id> = match item {
						Either::Left(node) => {
							if let Some(node) = strip_nodes_inner(
								old_nodes,
								node,
								visited,
								new_nodes,
								path_dependencies,
								strip,
							) {
								Either::Left(node)
							} else if let Ok(id) = reference.item().try_unwrap_object_ref() {
								Either::Right(id.clone())
							} else {
								return None;
							}
						},
						Either::Right(id) => Either::Right(id),
					};
					Some((reference, tg::Referent { item, path, tag }))
				})
				.collect();

			// Retain the contents if this is not a path dependency.
			let contents = if path_dependencies[node] {
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

		tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { target, .. }) => {
			new_nodes[new_node].replace(tg::lockfile::Node::Symlink(
				tg::lockfile::Symlink::Target {
					target: target.clone(),
				},
			));
		},
		tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, subpath }) => {
			// Remap the artifact if necessary.
			let artifact = match artifact {
				Either::Left(node) => strip_nodes_inner(
					old_nodes,
					node,
					visited,
					new_nodes,
					path_dependencies,
					strip,
				)
				.map(Either::Left),
				Either::Right(id) => Some(Either::Right(id)),
			};

			// Create the node.
			if let Some(artifact) = artifact {
				new_nodes[new_node].replace(tg::lockfile::Node::Symlink(
					tg::lockfile::Symlink::Artifact { artifact, subpath },
				));
			} else {
				new_nodes[new_node].replace(tg::lockfile::Node::Symlink(
					if let Some(subpath) = subpath {
						tg::lockfile::Symlink::Target { target: subpath }
					} else {
						return None;
					},
				));
			}
		},
	}

	Some(new_node)
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
			tg::lockfile::Node::Directory(directory) => {
				let it = directory
					.entries
					.values()
					.filter_map(|entry| entry.as_ref().left().copied());
				Box::new(it)
			},
			tg::lockfile::Node::File(file) => {
				let it = file
					.dependencies
					.values()
					.filter_map(|referent| referent.item.as_ref().left().copied());
				Box::new(it)
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, .. }) => {
				let it = artifact.as_ref().left().copied().into_iter();
				Box::new(it)
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { .. }) => {
				let it = None.into_iter();
				Box::new(it)
			},
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
	use insta::assert_json_snapshot;
	use tangram_client as tg;

	#[test]
	fn strip() {
		let lockfile = serde_json::from_str::<tg::Lockfile>(
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
		"#,
		)
		.unwrap();

		// Test that marking the nodes works.
		let mut strip = vec![true; lockfile.nodes.len()];
		super::mark_nodes(&lockfile.nodes, &mut strip);
		assert_eq!(&strip, &[false, false, true, false, false]);

		// Test that stripping the nodes works.
		let nodes = crate::Server::strip_lockfile_nodes(
			&lockfile.nodes,
			&[true, true, true, false, false],
			0,
		)
		.unwrap();
		assert_json_snapshot!(nodes, @r#"
		[
		  {
		    "kind": "directory",
		    "entries": {
		      "tangram.ts": 1
		    }
		  },
		  {
		    "kind": "file",
		    "dependencies": {
		      "a": {
		        "item": 2,
		        "tag": "a"
		      }
		    }
		  },
		  {
		    "kind": "directory",
		    "entries": {
		      "tangram.ts": 3
		    }
		  },
		  {
		    "kind": "file",
		    "contents": "blb_01038pab1jh9r3ztm2811kzr14ff3223xhcp9dgczg1gd1afmje6ng"
		  }
		]
		"#);
	}
}
