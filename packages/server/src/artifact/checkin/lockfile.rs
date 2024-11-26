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
			.await?;

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
			Err(ref error) if error.raw_os_error() == Some(libc::EPERM) => (),

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
		let mut nodes = Vec::new();
		for node in 0..graph.nodes.len() {
			let node = self.create_lockfile_node(graph, node).await?;
			nodes.push(node);
		}

		let root = *graph
			.paths
			.get(path)
			.ok_or_else(|| tg::error!("failed to get root object"))?;
		let nodes = self.strip_lockfile_nodes(&nodes, root)?;
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
			let id = graph.nodes[node].id.clone().unwrap().try_into()?;
			tg::Artifact::with_id(id).data(self).await?
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
				self.create_lockfile_symlink_node2(graph, node, &data).await
			},
		}
	}

	async fn create_lockfile_directory_node(
		&self,
		graph: &object::Graph,
		node: usize,
		_data: &tg::directory::Data,
	) -> tg::Result<tg::lockfile::Node> {
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
		Ok(tg::lockfile::Node::Directory { entries })
	}

	async fn create_lockfile_file_node(
		&self,
		graph: &object::Graph,
		node: usize,
		data: &tg::file::Data,
	) -> tg::Result<tg::lockfile::Node> {
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
		Ok(tg::lockfile::Node::File {
			contents: Some(contents),
			dependencies,
			executable,
		})
	}

	async fn create_lockfile_symlink_node2(
		&self,
		graph: &object::Graph,
		node: usize,
		data: &tg::symlink::Data,
	) -> tg::Result<tg::lockfile::Node> {
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
		let artifact = graph.nodes[node]
			.edges
			.first()
			.map(|edge| self.get_lockfile_entry(graph, edge.index));
		match artifact {
			Some(artifact) => Ok(tg::lockfile::Node::Symlink(
				tg::lockfile::Symlink::Artifact { artifact, subpath },
			)),
			None => {
				if let Some(subpath) = subpath {
					Ok(tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target {
						target: subpath,
					}))
				} else {
					Err(tg::error!(?data, "unable to determine subpath for lockfile"))
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
		let mut should_retain = vec![None; old_nodes.len()];
		check_if_references_module(old_nodes, None, root, &mut should_retain)?;

		// Strip nodes that don't reference tag dependencies.
		let mut new_nodes = Vec::with_capacity(old_nodes.len());
		let mut visited = vec![None; old_nodes.len()];
		strip_nodes_inner(
			old_nodes,
			root,
			&mut visited,
			&mut new_nodes,
			&should_retain,
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
				tg::lockfile::Node::Directory { entries } => {
					let retain = entries
						.keys()
						.any(|name| tg::package::is_module_path(name.as_ref()));
					visited[node].replace(retain);
					for (name, entry) in entries {
						let child_node = entry.as_ref().left().copied().unwrap();
						*visited[node].as_mut().unwrap() |= check_if_references_module(
							nodes,
							Some(name.as_ref()),
							child_node,
							visited,
						)?;
					}
				},
				tg::lockfile::Node::File { dependencies, .. } => {
					let retain =
						!dependencies.is_empty() || path.map_or(false, tg::package::is_module_path);
					visited[node].replace(retain);
					for (reference, referent) in dependencies {
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

// Recursively create a new list of nodes that have their path dependencies removed, while preserving nodes that transitively reference any tag dependencies.
fn strip_nodes_inner(
	old_nodes: &[tg::lockfile::Node],
	node: usize,
	visited: &mut Vec<Option<usize>>,
	new_nodes: &mut Vec<Option<tg::lockfile::Node>>,
	should_retain: &[Option<bool>],
) -> Option<usize> {
	if !should_retain[node].unwrap() {
		return None;
	}
	if let Some(visited) = visited[node] {
		return Some(visited);
	}

	let new_node = new_nodes.len();
	visited[node].replace(new_node);
	new_nodes.push(None);

	match old_nodes[node].clone() {
		tg::lockfile::Node::Directory { entries } => {
			let entries = entries
				.into_iter()
				.filter_map(|(name, entry)| {
					let entry = match entry {
						Either::Left(node) => {
							strip_nodes_inner(old_nodes, node, visited, new_nodes, should_retain)
								.map(Either::Left)
						},
						Either::Right(id) => Some(Either::Right(id)),
					};
					Some((name, entry?))
				})
				.collect();

			// Create a new node.
			new_nodes[new_node].replace(tg::lockfile::Node::Directory { entries });
		},
		tg::lockfile::Node::File {
			dependencies,
			executable,
			..
		} => {
			let dependencies = dependencies
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

			// Create the node.
			new_nodes[new_node].replace(tg::lockfile::Node::File {
				contents: None,
				dependencies,
				executable,
			});
		},

		tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { target }) => {
			new_nodes[new_node].replace(tg::lockfile::Node::Symlink(
				tg::lockfile::Symlink::Target {
					target: target.clone(),
				},
			));
		},
		tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, subpath }) => {
			// Remap the artifact if necessary.
			let artifact = match artifact {
				Either::Left(node) => {
					strip_nodes_inner(old_nodes, node, visited, new_nodes, should_retain)
						.map(Either::Left)
				},
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
				let tg::lockfile::Node::Directory { entries } = &nodes[node] else {
					return Err(tg::error!("invalid graph"));
				};
				let Some(entry) = entries.get(name) else {
					return Ok(None);
				};
				node = entry.as_ref().left().copied().unwrap();
			},
			_ => return Err(tg::error!(?item, ?subpath, "invalid referent")),
		}
	}
	Ok(Some(node))
}
