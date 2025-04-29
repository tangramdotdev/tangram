use crate::Server;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) fn create_lockfile(state: &super::State) -> tg::Result<tg::Lockfile> {
		let mut nodes = Vec::with_capacity(state.graph.nodes.len());
		let mut visited = vec![None; state.graph.nodes.len()];
		let root = Self::create_lockfile_node(state, 0, &mut nodes, &mut visited)?.unwrap_left();
		let nodes = nodes
			.into_iter()
			.map(|node| node.unwrap())
			.collect::<Vec<_>>();
		let nodes = Self::strip_lockfile_nodes(&nodes, root)?;
		Ok(tg::Lockfile { nodes })
	}

	pub(super) fn create_lockfile_node(
		state: &super::State,
		node: usize,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<Either<usize, tg::object::Id>> {
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
		data: &tg::directory::Data,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		let id = tg::directory::Id::new(&data.serialize()?);
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
		let directory = tg::lockfile::Directory {
			entries,
			id: Some(id),
		};
		Ok(tg::lockfile::Node::Directory(directory))
	}

	pub(super) fn checkin_create_lockfile_file_node(
		state: &super::State,
		index: usize,
		data: &tg::file::Data,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		let id = tg::file::Id::new(&data.serialize()?);
		let (contents, executable) = match data {
			tg::file::Data::Graph {
				graph: graph_id,
				node,
			} => {
				let graph = &state
					.graph_objects
					.iter()
					.find(|object| object.id == graph_id.clone().into())
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
			.map(|dependency| match dependency {
				super::FileDependency::Import {
					import,
					node,
					path,
					subpath,
				} => {
					let reference = import.reference.clone();
					let node =
						node.ok_or_else(|| tg::error!(%import = reference, "unresolved import"))?;
					let item = Self::create_lockfile_node(state, node, nodes, visited)?;
					let path = path.clone();
					let subpath = subpath.clone();
					let tag = state.graph.nodes[node].tag.clone();
					let referent = tg::Referent {
						item,
						path,
						subpath,
						tag,
					};
					Ok::<_, tg::Error>((reference, referent))
				},
				super::FileDependency::Referent {
					reference,
					referent,
				} => {
					let reference = reference.clone();
					let referent = tg::Referent {
						item: Either::Right(referent.item.clone()),
						path: referent.path.clone(),
						subpath: referent.subpath.clone(),
						tag: referent.tag.clone(),
					};
					Ok((reference, referent))
				},
			})
			.try_collect()?;
		let file = tg::lockfile::File {
			contents: Some(contents),
			dependencies,
			executable,
			id: Some(id),
		};
		Ok(tg::lockfile::Node::File(file))
	}

	fn checkin_create_lockfile_symlink_node(
		state: &super::State,
		_node: usize,
		data: &tg::symlink::Data,
		_nodes: &mut Vec<Option<tg::lockfile::Node>>,
		_visited: &mut [Option<Either<usize, tg::object::Id>>],
	) -> tg::Result<tg::lockfile::Node> {
		let id = tg::symlink::Id::new(&data.serialize()?);
		let subpath = match data {
			tg::symlink::Data::Graph {
				graph: graph_id,
				node,
			} => {
				let graph = &state
					.graph_objects
					.iter()
					.find(|object| object.id == graph_id.clone().into())
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
		let artifact = None; // todo: symlinks that point to artifacts.

		// Create the lockfile node.
		match artifact {
			Some(artifact) => Ok(tg::lockfile::Node::Symlink(
				tg::lockfile::Symlink::Artifact {
					artifact,
					id: Some(id),
					subpath,
				},
			)),
			None => {
				if let Some(subpath) = subpath {
					Ok(tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target {
						id: Some(id),
						target: subpath,
					}))
				} else {
					Err(tg::error!(
						?data,
						"unable to determine subpath for lockfile"
					))
				}
			},
		}
	}

	pub fn strip_lockfile_nodes(
		old_nodes: &[tg::lockfile::Node],
		root: usize,
	) -> tg::Result<Vec<tg::lockfile::Node>> {
		let mut references_module = vec![None; old_nodes.len()];
		check_if_references_module(old_nodes, None, root, &mut references_module)?;

		let mut is_tagged = vec![None; old_nodes.len()];
		check_if_transitively_tagged(old_nodes, false, root, &mut is_tagged)?;

		let mut in_graph = vec![false; old_nodes.len()];
		check_if_in_graph(old_nodes, &mut in_graph);

		// Strip nodes that don't reference tag dependencies.
		let mut new_nodes = Vec::with_capacity(old_nodes.len());
		let mut visited = vec![None; old_nodes.len()];
		strip_nodes_inner(
			old_nodes,
			root,
			&mut visited,
			&mut new_nodes,
			&references_module,
			&is_tagged,
			&in_graph,
		);

		// Construct a new lockfile with only stripped nodes.
		Ok(new_nodes.into_iter().map(Option::unwrap).collect())
	}
}

// Recursively compute the nodes that transitively reference tag dependencies.
#[allow(clippy::match_on_vec_items)]
fn check_if_references_module(
	nodes: &[tg::lockfile::Node],
	path: Option<&Path>,
	node: usize,
	visited: &mut Vec<Option<bool>>,
) -> tg::Result<bool> {
	match visited[node] {
		None => {
			match &nodes[node] {
				tg::lockfile::Node::Directory(directory) => {
					let retain = directory
						.entries
						.keys()
						.any(|name| tg::package::is_module_path(name.as_ref()));
					visited[node].replace(retain);
					for (name, entry) in &directory.entries {
						let child_node = entry.as_ref().left().copied().unwrap();
						*visited[node].as_mut().unwrap() |= check_if_references_module(
							nodes,
							Some(name.as_ref()),
							child_node,
							visited,
						)?;
					}
				},
				tg::lockfile::Node::File(file) => {
					let retain = !file.dependencies.is_empty()
						|| path.is_some_and(tg::package::is_module_path);
					visited[node].replace(retain);
					for (reference, referent) in &file.dependencies {
						let path = reference
							.item()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.options()?.path.as_ref())
							.map(AsRef::as_ref);
						let Either::Left(child_node) = &referent.item else {
							continue;
						};
						*visited[node].as_mut().unwrap() |=
							check_if_references_module(nodes, path, *child_node, visited)?;
					}
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
					artifact,
					subpath,
					..
				}) => {
					let retain = subpath
						.as_ref()
						.is_some_and(|subpath| tg::package::is_module_path(subpath));
					visited[node].replace(retain);
					if let Some(child_node) =
						try_find_in_lockfile_nodes(nodes, artifact, subpath.as_deref())?
					{
						let path = subpath.as_ref().map(PathBuf::as_path);
						*visited[node].as_mut().unwrap() |=
							check_if_references_module(nodes, path, child_node, visited)?;
					}
				},
				tg::lockfile::Node::Symlink(_) => {
					visited[node].replace(false);
				},
			}
			Ok(visited[node].unwrap())
		},
		Some(mark) => Ok(mark),
	}
}

// Recursively compute the file nodes whose contents should be retained
#[allow(clippy::match_on_vec_items)]
fn check_if_transitively_tagged(
	nodes: &[tg::lockfile::Node],
	referrer_tagged: bool,
	node: usize,
	visited: &mut Vec<Option<bool>>,
) -> tg::Result<()> {
	match visited[node] {
		None => match &nodes[node] {
			tg::lockfile::Node::Directory(directory) => {
				visited[node].replace(referrer_tagged);
				for entry in directory.entries.values() {
					let child_node = entry.as_ref().left().copied().unwrap();
					check_if_transitively_tagged(nodes, referrer_tagged, child_node, visited)?;
				}
			},
			tg::lockfile::Node::File(file) => {
				visited[node].replace(referrer_tagged);
				for referent in file.dependencies.values() {
					let Either::Left(child_node) = &referent.item else {
						continue;
					};
					let referrer_tagged = referent.tag.is_some() || referrer_tagged;
					check_if_transitively_tagged(nodes, referrer_tagged, *child_node, visited)?;
				}
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
				artifact,
				subpath,
				..
			}) => {
				visited[node].replace(referrer_tagged);
				if let Some(child_node) =
					try_find_in_lockfile_nodes(nodes, artifact, subpath.as_deref())?
				{
					check_if_transitively_tagged(nodes, referrer_tagged, child_node, visited)?;
				}
			},
			tg::lockfile::Node::Symlink(_) => {
				visited[node].replace(false);
			},
		},
		Some(false) if referrer_tagged => {
			visited[node].replace(true);
		},
		_ => (),
	}
	Ok(())
}

fn check_if_in_graph(nodes: &[tg::lockfile::Node], visited: &mut [bool]) {
	for scc in petgraph::algo::tarjan_scc(&LockfileGraphImpl(nodes)) {
		let in_graph = scc.len() > 1;
		for node in scc {
			visited[node] = in_graph;
		}
	}
}

fn strip_nodes_inner(
	old_nodes: &[tg::lockfile::Node],
	node: usize,
	visited: &mut Vec<Option<usize>>,
	new_nodes: &mut Vec<Option<tg::lockfile::Node>>,
	should_retain: &[Option<bool>],
	is_tagged: &[Option<bool>],
	in_graph: &[bool],
) -> Option<usize> {
	// Strip nodes that don't reference modules, are untagged, and not contained within graphs.
	if !(matches!(should_retain[node], Some(true))
		|| matches!(is_tagged[node], Some(true))
		|| in_graph[node])
	{
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
							should_retain,
							is_tagged,
							in_graph,
						)
						.map(Either::Left),
						Either::Right(id) => Some(Either::Right(id)),
					};
					Some((name, entry?))
				})
				.collect();

			// Create a new node.
			let directory = tg::lockfile::Directory {
				entries,
				id: directory.id.clone(),
			};
			new_nodes[new_node].replace(tg::lockfile::Node::Directory(directory));
		},
		tg::lockfile::Node::File(file) => {
			let dependencies = file
				.dependencies
				.into_iter()
				.filter_map(|(reference, referent)| {
					let tg::Referent {
						item,
						path,
						subpath,
						tag,
					} = referent;

					// Special case, the reference is by ID.
					let item = match item {
						Either::Left(node) => {
							if let Some(node) = strip_nodes_inner(
								old_nodes,
								node,
								visited,
								new_nodes,
								should_retain,
								is_tagged,
								in_graph,
							) {
								Either::Left(node)
							} else if reference
								.item()
								.try_unwrap_path_ref()
								.ok()
								.or_else(|| reference.options()?.path.as_ref())
								.is_none()
							{
								let id = old_nodes[node].id()?.into();
								Either::Right(id)
							} else {
								return None;
							}
						},
						Either::Right(id) => Either::Right(id),
					};
					Some((
						reference,
						tg::Referent {
							item,
							path,
							subpath,
							tag,
						},
					))
				})
				.collect();

			// Retain the contents if necessary.
			let retain_contents = matches!(is_tagged[node], Some(true)) || in_graph[node];
			let contents = if retain_contents { file.contents } else { None };

			// Create the node.
			let file = tg::lockfile::File {
				contents,
				dependencies,
				executable: file.executable,
				id: file.id,
			};
			new_nodes[new_node].replace(tg::lockfile::Node::File(file));
		},

		tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { target, .. }) => {
			new_nodes[new_node].replace(tg::lockfile::Node::Symlink(
				tg::lockfile::Symlink::Target {
					target: target.clone(),
					id: None,
				},
			));
		},
		tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
			artifact,
			id,
			subpath,
		}) => {
			// Remap the artifact if necessary.
			let artifact = match artifact {
				Either::Left(node) => strip_nodes_inner(
					old_nodes,
					node,
					visited,
					new_nodes,
					should_retain,
					is_tagged,
					in_graph,
				)
				.map(Either::Left),
				Either::Right(id) => Some(Either::Right(id)),
			};

			// Create the node.
			if let Some(artifact) = artifact {
				new_nodes[new_node].replace(tg::lockfile::Node::Symlink(
					tg::lockfile::Symlink::Artifact {
						artifact,
						subpath,
						id,
					},
				));
			} else {
				new_nodes[new_node].replace(tg::lockfile::Node::Symlink(
					if let Some(subpath) = subpath {
						tg::lockfile::Symlink::Target {
							target: subpath,
							id,
						}
					} else {
						return None;
					},
				));
			}
		},
	}

	Some(new_node)
}

fn try_find_in_lockfile_nodes(
	nodes: &[tg::lockfile::Node],
	item: &Either<usize, tg::object::Id>,
	subpath: Option<&Path>,
) -> tg::Result<Option<usize>> {
	let Some(mut node) = item.as_ref().left().copied() else {
		return Ok(None);
	};
	let Some(subpath) = &subpath else {
		return Ok(Some(node));
	};
	for component in subpath.components() {
		match component {
			std::path::Component::CurDir => (),
			std::path::Component::Normal(normal) => {
				let name = normal.to_str().unwrap();
				let tg::lockfile::Node::Directory(directory) = &nodes[node] else {
					return Err(tg::error!("invalid graph"));
				};
				let Some(entry) = directory.entries.get(name) else {
					return Ok(None);
				};
				node = entry.as_ref().left().copied().unwrap();
			},
			_ => return Err(tg::error!(?item, ?subpath, "invalid referent")),
		}
	}
	Ok(Some(node))
}

struct LockfileGraphImpl<'a>(&'a [tg::lockfile::Node]);

impl petgraph::visit::GraphBase for LockfileGraphImpl<'_> {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

#[allow(clippy::needless_arbitrary_self_type)]
impl<'a> petgraph::visit::NodeIndexable for &'a LockfileGraphImpl<'a> {
	fn from_index(self: &Self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(self: &Self) -> usize {
		self.0.len()
	}

	fn to_index(self: &Self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a LockfileGraphImpl<'a> {
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

impl<'a> petgraph::visit::IntoNodeIdentifiers for &'a LockfileGraphImpl<'a> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.len()
	}
}
