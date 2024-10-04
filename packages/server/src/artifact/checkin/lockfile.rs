use super::{
	input,
	unify::{self, Id},
};
use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt,
	path::PathBuf,
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;
use tg::path::Ext as _;

impl Server {
	// Create one giant lockfile for the entire graph, and return a table of nodes within that lockfile that correspond to path dependencies.
	pub(super) async fn create_lockfile(
		&self,
		graph: &unify::Graph,
		root: &Id,
		progress: &super::ProgressState,
	) -> tg::Result<(tg::Lockfile, BTreeMap<PathBuf, usize>)> {
		let mut paths = BTreeMap::new();

		// Compute the new lockfile IDs and paths.
		let mut stack = Vec::with_capacity(graph.nodes.len());
		stack.push(root);
		let mut ids = BTreeMap::new();
		let mut counter = 0;
		while let Some(old_id) = stack.pop() {
			if ids.contains_key(old_id) {
				continue;
			}
			let node = graph
				.nodes
				.get(old_id)
				.ok_or_else(|| tg::error!("missing node in graph"))?;
			match &node.object {
				Either::Left(input) => {
					// Get a new index for this node.
					let index = counter;
					counter += 1;
					ids.insert(old_id, Either::Left(index));

					// Add the path of this node to the paths table.
					let input = input.read().await;
					paths.insert(input.arg.path.clone(), index);
				},
				Either::Right(object) => {
					ids.insert(old_id, Either::Right(object.clone()));
				},
			}
			stack.extend(node.outgoing.values());
		}

		// Compute the new dependencies for each node.
		let dependencies = ids
			.iter()
			.filter_map(|(old_id, new_id)| {
				if new_id.is_right() {
					return None;
				}
				let node = graph.nodes.get(old_id).unwrap();
				let dependencies = node
					.outgoing
					.iter()
					.map(|(reference, old_id)| {
						let object = ids.get(old_id).unwrap().clone();
						let tag = graph.nodes.get(old_id).unwrap().tag.clone();
						let dependency = tg::lockfile::Dependency { object, tag };
						(reference.clone(), dependency)
					})
					.collect::<BTreeMap<_, _>>();
				Some((*old_id, dependencies))
			})
			.collect::<BTreeMap<_, _>>();

		// Create the nodes concurrently.
		let mut nodes = dependencies
			.into_iter()
			.map(|(old_id, dependencies)| async {
				let node = graph.nodes.get(old_id).unwrap();
				let input = node.object.as_ref().unwrap_left().read().await.clone();

				let node = if input.metadata.is_dir() {
					let entries = dependencies
						.iter()
						.map(|(reference, dependency)| {
							let name = reference
								.path()
								.try_unwrap_path_ref()
								.map_err(
									|_| tg::error!(%reference, "invalid input graph, expected a path"),
								)?
								.components()
								.last()
								.ok_or_else(
									|| tg::error!(%reference, "invalid input graph, expected a non-empty path"),
								)?;
							let std::path::Component::Normal(name) = name else {
								return Err(
									tg::error!(%reference, "expected a normal path component"),
								);
							};
							let name = name
								.to_str()
								.ok_or_else(|| tg::error!("invalid path"))?
								.to_owned();
							let id = match &dependency.object {
								Either::Left(id) => Either::Left(*id),
								Either::Right(tg::object::Id::Directory(id)) => {
									Either::Right(id.clone().into())
								},
								Either::Right(tg::object::Id::File(id)) => {
									Either::Right(id.clone().into())
								},
								Either::Right(tg::object::Id::Symlink(id)) => {
									Either::Right(id.clone().into())
								},
								Either::Right(object) => {
									return Err(tg::error!(
										%kind = object.kind(),
										"invalid input graph, expected an artifact"
									))
								},
							};
							Ok::<_, tg::Error>((name, id))
						})
						.try_collect()?;
					tg::lockfile::Node::Directory { entries }
				} else if input.metadata.is_file() {
					self.create_lockfile_file_node(input, dependencies, progress)
						.await?
				} else if input.metadata.is_symlink() {
					self.create_lockfile_symlink_node(input).await?
				} else {
					return Err(tg::error!("unknown file type"));
				};
				let index = *ids.get(old_id).unwrap().as_ref().unwrap_left();
				Ok::<_, tg::Error>((index, node))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		nodes.sort_unstable_by_key(|(k, _)| *k);
		let nodes = nodes.into_iter().map(|(_, n)| n).collect();

		// Create the lockfile.
		let lockfile = tg::Lockfile { nodes };

		Ok((lockfile, paths))
	}

	async fn create_lockfile_file_node(
		&self,
		input: input::Graph,
		dependencies: BTreeMap<tg::Reference, tg::lockfile::Dependency>,
		progress: &super::ProgressState,
	) -> tg::Result<tg::lockfile::Node> {
		let input::Graph { arg, metadata, .. } = input;

		// Read the file contents.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let file = tokio::fs::File::open(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read file"))?;
		let output = self
			.create_blob_inner(file, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create blob"))?;
		drop(permit);

		// Sanity check.
		if !dependencies.is_empty() && input.edges.is_empty() {
			return Err(tg::error!("invalid input"));
		}

		let contents = Some(output.blob);
		let executable = metadata.permissions().mode() & 0o111 != 0;

		// Update state.
		progress.report_blobs_progress();

		// Create the data.
		Ok(tg::lockfile::Node::File {
			contents,
			dependencies,
			executable,
		})
	}

	async fn create_lockfile_symlink_node(
		&self,
		input: input::Graph,
	) -> tg::Result<tg::lockfile::Node> {
		let input::Graph { arg, .. } = input;
		let path = arg.path.clone();

		// Read the target from the symlink.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let target = tokio::fs::read_link(&path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), r#"failed to read the symlink at path"#,),
		)?;
		drop(permit);

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::template::Data::unrender(artifacts_path, target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(Either::Right(artifact.into())), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		Ok(tg::lockfile::Node::Symlink { artifact, path })
	}

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
			let root = *paths
				.get(&input.read().await.arg.path)
				.ok_or_else(|| tg::error!("missing workspace root in lockfile"))?;
			let stripped_lockfile = self.strip_lockfile(lockfile, root)?;
			let contents = serde_json::to_string_pretty(&stripped_lockfile)
				.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let lockfile_path = arg.path.join(tg::package::LOCKFILE_FILE_NAME).normalize();
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

	pub fn strip_lockfile(&self, lockfile: &tg::Lockfile, root: usize) -> tg::Result<tg::Lockfile> {
		// Compute whether the nodes have transitive tag dependencies.
		let mut has_tag_dependencies_ = vec![None; lockfile.nodes.len()];
		has_tag_dependencies(lockfile, root, &mut has_tag_dependencies_);

		// Strip nodes that don't reference tag dependencies.
		let mut new_nodes = Vec::with_capacity(lockfile.nodes.len());
		let mut visited = vec![None; lockfile.nodes.len()];
		strip_nodes(
			lockfile,
			root,
			&mut visited,
			&mut new_nodes,
			&has_tag_dependencies_,
		);

		// Construct a new lockfile with only stripped nodes.
		let nodes = new_nodes.into_iter().map(Option::unwrap).collect();
		Ok(tg::Lockfile { nodes })
	}
}

// Return a list of neighbors, and whether the neighbor is referenced by tag.
fn neighbors(lockfile: &tg::Lockfile, node: usize) -> Vec<(usize, bool)> {
	match &lockfile.nodes[node] {
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
	lockfile: &tg::Lockfile,
	node: usize,
	visited: &mut Vec<Option<bool>>,
) -> bool {
	match visited[node] {
		None => {
			visited[node].replace(false);
			for (neighbor, is_tag_dependency) in neighbors(lockfile, node) {
				if is_tag_dependency {
					visited[node].replace(true);
				}
				if has_tag_dependencies(lockfile, neighbor, visited) {
					visited[node].replace(true);
				}
			}
			visited[node].unwrap()
		},
		Some(mark) => mark,
	}
}

// Recursively create a new list of nodes that have their path dependencies removed, while preserving nodes that transitively reference any tag dependencies.
fn strip_nodes(
	lockfile: &tg::Lockfile,
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

	match lockfile.nodes[node].clone() {
		tg::lockfile::Node::Directory { entries } => {
			let entries = entries
				.into_iter()
				.filter_map(|(name, entry)| {
					let entry = match entry {
						Either::Left(node) => {
							strip_nodes(lockfile, node, visited, new_nodes, has_tag_dependencies)
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
				.filter_map(|(reference, dependency)| {
					let object = match dependency.object {
						Either::Left(node) => Either::Left(strip_nodes(
							lockfile,
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
					strip_nodes(lockfile, node, visited, new_nodes, has_tag_dependencies)
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
