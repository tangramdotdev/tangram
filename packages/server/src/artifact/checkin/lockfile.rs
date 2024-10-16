use super::{input, object};
use crate::Server;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::PathBuf,
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) async fn create_lockfile(&self, graph: &object::Graph) -> tg::Result<tg::Lockfile> {
		let mut nodes = Vec::new();
		for node in 0..graph.nodes.len() {
			let node = self.create_lockfile_node(graph, node).await?;
			nodes.push(node);
		}
		let nodes = self.strip_nodes(&nodes, 0);
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
					.path()
					.unwrap_path_ref()
					.components()
					.next()
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
				let object = self.get_lockfile_entry(graph, edge.index);
				let dependency = tg::lockfile::Dependency { object, tag };
				(reference, dependency)
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
		let path = match data {
			tg::symlink::Data::Graph { graph, node } => {
				let symlink = tg::Graph::with_id(graph.clone()).object(self).await?.nodes[*node]
					.clone()
					.try_unwrap_symlink()
					.unwrap();
				symlink.path.map(PathBuf::from)
			},
			tg::symlink::Data::Normal { path, .. } => path.as_ref().map(PathBuf::from),
		};
		let artifact = graph.nodes[node]
			.edges
			.first()
			.map(|edge| self.get_lockfile_entry(graph, edge.index));
		Ok(tg::lockfile::Node::Symlink { artifact, path })
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
}

impl Server {
	pub(super) async fn write_lockfiles(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		lockfile: &tg::Lockfile,
		paths: &BTreeMap<PathBuf, usize>,
	) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		self.write_lockfiles_inner(input, lockfile, paths, &mut visited)
			.await
	}

	pub(super) async fn write_lockfiles_inner(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		lockfile: &tg::Lockfile,
		paths: &BTreeMap<PathBuf, usize>,
		visited: &mut BTreeSet<PathBuf>,
	) -> tg::Result<()> {
		let input::Graph {
			arg,
			metadata,
			edges,
			..
		} = input.read().await.clone();

		if visited.contains(&arg.path) {
			return Ok(());
		}
		visited.insert(arg.path.clone());

		if metadata.is_dir()
			&& tg::package::try_get_root_module_file_name_for_package_path(arg.path.as_ref())
				.await?
				.is_some()
		{
			let contents = serde_json::to_string_pretty(&lockfile)
				.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let lockfile_path = arg.path.join(tg::package::LOCKFILE_FILE_NAME);
			tokio::fs::write(&lockfile_path, &contents)
				.await
				.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
			tokio::fs::write(&lockfile_path, &contents)
				.await
				.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
			return Ok(());
		}

		let children = edges
			.iter()
			.filter_map(input::Edge::node)
			.collect::<Vec<_>>();

		for child in children {
			// Skip any paths outside the workspace.
			if child.read().await.root.is_none() {
				continue;
			}
			Box::pin(self.write_lockfiles_inner(child, lockfile, paths, visited)).await?;
		}

		Ok(())
	}

	#[allow(clippy::unused_self)]
	fn strip_nodes(
		&self,
		old_nodes: &[tg::lockfile::Node],
		root: usize,
	) -> Vec<tg::lockfile::Node> {
		// Compute whether the nodes have transitive tag dependencies.
		let mut has_tag_dependencies_ = vec![None; old_nodes.len()];
		has_tag_dependencies(old_nodes, root, &mut has_tag_dependencies_);

		// Strip nodes that don't reference tag dependencies.
		let mut new_nodes = Vec::with_capacity(old_nodes.len());
		let mut visited = vec![None; old_nodes.len()];
		strip_nodes_inner(
			old_nodes,
			root,
			&mut visited,
			&mut new_nodes,
			&has_tag_dependencies_,
		);

		// Construct a new lockfile with only stripped nodes.
		new_nodes.into_iter().map(Option::unwrap).collect()
	}
}

// Return a list of neighbors, and whether the neighbor is referenced by tag.
fn neighbors(nodes: &[tg::lockfile::Node], node: usize) -> Vec<(usize, bool)> {
	match &nodes[node] {
		tg::lockfile::Node::Directory { entries } => entries
			.values()
			.filter_map(|entry| {
				let node = *entry.as_ref().left()?;
				Some((node, false))
			})
			.collect(),
		tg::lockfile::Node::File { dependencies, .. } => dependencies
			.values()
			.filter_map(|dependency| {
				let node = *dependency.object.as_ref().left()?;
				Some((node, dependency.tag.is_some()))
			})
			.collect(),
		tg::lockfile::Node::Symlink { artifact, .. } => {
			if let Some(Either::Left(node)) = artifact {
				vec![(*node, false)]
			} else {
				Vec::new()
			}
		},
	}
}

// Recursively compute the nodes that transitively reference tag dependencies.
#[allow(clippy::match_on_vec_items)]
fn has_tag_dependencies(
	nodes: &[tg::lockfile::Node],
	node: usize,
	visited: &mut Vec<Option<bool>>,
) -> bool {
	match visited[node] {
		None => {
			visited[node].replace(false);
			for (neighbor, is_tag_dependency) in neighbors(nodes, node) {
				if is_tag_dependency {
					visited[node].replace(true);
				}
				if has_tag_dependencies(nodes, neighbor, visited) {
					visited[node].replace(true);
				}
			}
			visited[node].unwrap()
		},
		Some(mark) => mark,
	}
}

// Recursively create a new list of nodes that have their path dependencies removed, while preserving nodes that transitively reference any tag dependencies.
fn strip_nodes_inner(
	old_nodes: &[tg::lockfile::Node],
	node: usize,
	visited: &mut Vec<Option<usize>>,
	new_nodes: &mut Vec<Option<tg::lockfile::Node>>,
	has_tag_dependencies: &[Option<bool>],
) -> Option<usize> {
	if !has_tag_dependencies[node].unwrap() {
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
						Either::Left(node) => strip_nodes_inner(
							old_nodes,
							node,
							visited,
							new_nodes,
							has_tag_dependencies,
						)
						.map(Either::Left),
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
				.filter_map(|(reference, dependency)| {
					let object = match dependency.object {
						Either::Left(node) => Either::Left(strip_nodes_inner(
							old_nodes,
							node,
							visited,
							new_nodes,
							has_tag_dependencies,
						)?),
						Either::Right(id) => Either::Right(id),
					};
					let tag = dependency.tag;
					Some((reference, tg::lockfile::Dependency { object, tag }))
				})
				.collect();

			// Create the node.
			new_nodes[new_node].replace(tg::lockfile::Node::File {
				contents: None,
				dependencies,
				executable,
			});
		},

		tg::lockfile::Node::Symlink { artifact, path } => {
			// Remap the artifact if necessary.
			let artifact = match artifact {
				Some(Either::Left(node)) => {
					strip_nodes_inner(old_nodes, node, visited, new_nodes, has_tag_dependencies)
						.map(Either::Left)
				},
				Some(Either::Right(id)) => Some(Either::Right(id)),
				None => None,
			};

			// Create the node.
			new_nodes[new_node].replace(tg::lockfile::Node::Symlink { artifact, path });
		},
	}

	Some(new_node)
}
