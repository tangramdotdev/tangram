use super::{input, object};
use crate::Server;
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	/// Attempt to write a lockfile. If the lockfile is empty, the destination is read-only, this is a no-op.
	pub(super) async fn try_write_lockfile(
		&self,
		input: &input::Graph,
		object: &object::Graph,
	) -> tg::Result<()> {
		// Create the lockfile.
		let lockfile = self
			.create_lockfile(object, &input.nodes[0].arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the lockfile"))?;

		// Skip empty lockfiles.
		if lockfile.nodes.is_empty() {
			return Ok(());
		};

		// Get the path to the root of the input graph.
		let root_path = &input.nodes[0].arg.path;

		// If the root item is a directory, the lockfile goes within it. Otherwise, write it next to the file.
		let lockfile_path = if input.nodes[0].metadata.is_dir() {
			root_path.join(tg::package::LOCKFILE_FILE_NAME)
		} else {
			root_path
				.parent()
				.unwrap()
				.join(tg::package::LOCKFILE_FILE_NAME)
		};

		// Serialize the lockfile.
		let lockfile = serde_json::to_vec_pretty(&lockfile)
			.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;

		// Write to disk.
		match tokio::fs::write(&lockfile_path, &lockfile).await {
			Ok(()) => (),

			// If there is an EPERM because the destination is RO, ignore.
			Err(error) if error.raw_os_error() == Some(libc::EPERM) => (),

			// Return all other errors.
			Err(source) => {
				return Err(
					tg::error!(!source, %path = lockfile_path.display(), "failed to write lockfile"),
				);
			},
		}

		Ok(())
	}

	async fn create_lockfile(
		&self,
		graph: &object::Graph,
		path: &Path,
	) -> tg::Result<tg::Lockfile> {
		// Create the lockfile nodes.
		let mut nodes = Vec::with_capacity(graph.nodes.len());
		for node in 0..graph.nodes.len() {
			let node = self
				.create_lockfile_node(graph, node)
				.await
				.map_err(|source| tg::error!(!source, %node,"failed to create lockfile node"))?;
			nodes.push(node);
		}

		// Find the root index.
		let root = *graph
			.paths
			.get(path)
			.ok_or_else(|| tg::error!("failed to get root object"))?;

		// Strip the lockfile nodes.
		let nodes = self
			.strip_lockfile_nodes(&nodes, root)
			.map_err(|source| tg::error!(!source, "failed to strip the lockfile"))?;

		Ok(tg::Lockfile { nodes })
	}

	async fn create_lockfile_node(
		&self,
		graph: &object::Graph,
		node: usize,
	) -> tg::Result<tg::lockfile::Node> {
		let data = if let Some(data) = graph.nodes[node].data.as_ref() {
			data.clone()
		} else {
			let id: tg::artifact::Id = graph.nodes[node]
				.id
				.clone()
				.unwrap()
				.try_into()
				.map_err(|_| tg::error!(%node, "expected an artifact"))?;
			tg::Artifact::with_id(id.clone()).data(self).await.map_err(
				|source| tg::error!(!source, %artifact = id, "missing artifact in object graph"),
			)?
		};

		match data {
			tg::artifact::Data::Directory(data) => {
				self.create_lockfile_directory_node(graph, node, &data)
					.await
			},
			tg::artifact::Data::File(data) => {
				self.create_lockfile_file_node(graph, node, &data).await
			},
			tg::artifact::Data::Symlink(data) => {
				self.create_lockfile_symlink_node(graph, node, &data).await
			},
		}
	}

	async fn create_lockfile_directory_node(
		&self,
		graph: &object::Graph,
		node: usize,
		data: &tg::directory::Data,
	) -> tg::Result<tg::lockfile::Node> {
		let id = tg::directory::Id::new(&data.serialize()?);
		let entries = graph.nodes[node]
			.edges
			.iter()
			.map(|edge| {
				let name = edge
					.reference
					.item()
					.unwrap_path_ref()
					.components()
					.last()
					.unwrap()
					.as_os_str()
					.to_str()
					.unwrap()
					.to_owned();
				let entry = self.get_lockfile_entry(graph, edge.index);
				(name, entry)
			})
			.collect();
		let directory = tg::lockfile::Directory {
			entries,
			id: Some(id),
		};
		Ok(tg::lockfile::Node::Directory(directory))
	}

	async fn create_lockfile_file_node(
		&self,
		graph: &object::Graph,
		node: usize,
		data: &tg::file::Data,
	) -> tg::Result<tg::lockfile::Node> {
		let id = tg::file::Id::new(&data.serialize()?);
		let (contents, executable) = match data {
			tg::file::Data::Graph { graph, node } => {
				let file = tg::Graph::with_id(graph.clone()).object(self).await?.nodes[*node]
					.clone()
					.try_unwrap_file()
					.unwrap();
				let contents = file.contents.id(self).await?;
				let executable = file.executable;
				(contents, executable)
			},
			tg::file::Data::Normal {
				contents,
				executable,
				..
			} => (contents.clone(), *executable),
		};
		let dependencies = graph.nodes[node]
			.edges
			.iter()
			.map(|edge| {
				let reference = edge.reference.clone();
				let tag = edge.tag.clone();
				let item = self.get_lockfile_entry(graph, edge.index);
				let path = edge.path.clone();
				let subpath = edge.subpath.clone();
				let referent = tg::Referent {
					item,
					path,
					subpath,
					tag,
				};
				(reference, referent)
			})
			.collect();
		let file = tg::lockfile::File {
			contents: Some(contents),
			dependencies,
			executable,
			id: Some(id),
		};
		Ok(tg::lockfile::Node::File(file))
	}

	async fn create_lockfile_symlink_node(
		&self,
		graph: &object::Graph,
		node: usize,
		data: &tg::symlink::Data,
	) -> tg::Result<tg::lockfile::Node> {
		let id = tg::symlink::Id::new(&data.serialize()?);
		let subpath = match data {
			tg::symlink::Data::Graph { graph, node } => {
				let symlink = tg::Graph::with_id(graph.clone()).object(self).await?.nodes[*node]
					.clone()
					.try_unwrap_symlink()
					.unwrap();
				match symlink {
					tg::graph::object::Symlink::Artifact {
						artifact: _,
						subpath,
					} => subpath.map(PathBuf::from),
					tg::graph::object::Symlink::Target { target } => Some(target),
				}
			},
			tg::symlink::Data::Target { target } => Some(PathBuf::from(target)),
			tg::symlink::Data::Artifact {
				artifact: _,
				subpath,
			} => subpath.clone().map(PathBuf::from),
		};

		// The artifact is either another point in the graph, or referred to explicitly by data.
		let artifact = graph.nodes[node]
			.edges
			.first()
			.map(|edge| self.get_lockfile_entry(graph, edge.index))
			.or_else(|| match data {
				tg::symlink::Data::Artifact { artifact, .. } => {
					Some(Either::Right(artifact.clone().into()))
				},
				_ => None,
			});

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

	#[allow(clippy::unused_self)]
	fn get_lockfile_entry(
		&self,
		graph: &object::Graph,
		node: usize,
	) -> Either<usize, tg::object::Id> {
		match &graph.nodes[node].unify.object {
			Either::Left(_) => Either::Left(node),
			Either::Right(id) => Either::Right(id.clone()),
		}
	}

	#[allow(clippy::unused_self)]
	pub fn strip_lockfile_nodes(
		&self,
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
					for (name, entry) in directory.entries.iter() {
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
						|| path.map_or(false, tg::package::is_module_path);
					visited[node].replace(retain);
					for (reference, referent) in file.dependencies.iter() {
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
						.map_or(false, |subpath| tg::package::is_module_path(subpath));
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
					let item = match item {
						Either::Left(node) => Either::Left(strip_nodes_inner(
							old_nodes,
							node,
							visited,
							new_nodes,
							should_retain,
							is_tagged,
							in_graph,
						)?),
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
			std::path::Component::CurDir => continue,
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

impl<'a> petgraph::visit::GraphBase for LockfileGraphImpl<'a> {
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
