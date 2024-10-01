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
	pub(super) async fn create_lockfile(
		&self,
		graph: &unify::Graph,
		root: &Id,
		progress: &super::ProgressState,
	) -> tg::Result<tg::Lockfile> {
		// Get the path of the root item.
		let root_path = graph
			.nodes
			.get(root)
			.unwrap()
			.object
			.as_ref()
			.unwrap_left()
			.read()
			.await
			.arg
			.path
			.clone();

		// Compute the new lockfile IDs and paths.
		let mut stack = Vec::with_capacity(graph.nodes.len());
		stack.push(root);
		let mut ids = BTreeMap::new();
		let mut paths: BTreeMap<PathBuf, Vec<usize>> = BTreeMap::new();
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
					let index = counter;
					counter += 1;

					let input = input.read().await;
					let path = input.arg.path.diff(&root_path).unwrap();
					paths.entry(path).or_default().push(index);

					ids.insert(old_id, Either::Left(index));
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
						let object = ids.get(old_id).cloned();
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
								Some(Either::Left(id)) => Either::Left(*id),
								Some(Either::Right(tg::object::Id::Directory(id))) => {
									Either::Right(id.clone().into())
								},
								Some(Either::Right(tg::object::Id::File(id))) => {
									Either::Right(id.clone().into())
								},
								Some(Either::Right(tg::object::Id::Symlink(id))) => {
									Either::Right(id.clone().into())
								},
								_ => {
									return Err(tg::error!(
										"invalid input graph, expected an artifact"
									))
								},
							};
							Ok::<_, tg::Error>((name, Some(id)))
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
		Ok(tg::Lockfile { paths, nodes })
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
			(Some(Some(Either::Right(artifact.into()))), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		Ok(tg::lockfile::Node::Symlink { artifact, path })
	}

	pub(super) async fn write_lockfiles(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		lockfile: &tg::Lockfile,
	) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		self.write_lockfiles_inner(input, lockfile, &mut visited)
			.await
	}

	pub(super) async fn write_lockfiles_inner(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		lockfile: &tg::Lockfile,
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
			let root = *lockfile
				.paths
				.get(&PathBuf::from("."))
				.ok_or_else(|| tg::error!("missing workspace root in lockfile"))?
				.first()
				.unwrap();
			let stripped_lockfile = self.strip_lockfile(lockfile, root).await?;
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
			Box::pin(self.write_lockfiles_inner(child, lockfile, visited)).await?;
		}

		Ok(())
	}

	#[allow(clippy::unused_async)]
	async fn strip_lockfile(
		&self,
		lockfile: &tg::Lockfile,
		root: usize,
	) -> tg::Result<tg::Lockfile> {
		let mut nodes = Vec::with_capacity(lockfile.nodes.len());
		let mut paths = BTreeMap::new();
		let mut visited = BTreeMap::new();
		let old_paths = lockfile
			.paths
			.iter()
			.flat_map(|(path, nodes)| nodes.iter().copied().map(move |node| (node, path)))
			.collect::<BTreeMap<_, _>>();
		self.strip_lockfile_inner(
			lockfile,
			&old_paths,
			root,
			&mut nodes,
			&mut paths,
			&mut visited,
		)?;
		Ok(tg::Lockfile { paths, nodes })
	}

	#[allow(clippy::only_used_in_recursion)]
	fn strip_lockfile_inner(
		&self,
		lockfile: &tg::Lockfile,
		old_paths: &BTreeMap<usize, &PathBuf>,
		node: usize,
		nodes: &mut Vec<tg::lockfile::Node>,
		paths: &mut BTreeMap<PathBuf, Vec<usize>>,
		visited: &mut BTreeMap<usize, Option<usize>>,
	) -> tg::Result<Option<usize>> {
		if let Some(visited) = visited.get(&node).copied() {
			return Ok(visited);
		}

		match &lockfile.nodes[node] {
			tg::lockfile::Node::Directory { entries } => {
				let mut entries_ = BTreeMap::new();
				for (name, entry) in entries {
					let entry = match entry {
						Some(Either::Left(node)) => {
							let entry = self.strip_lockfile_inner(
								lockfile, old_paths, *node, nodes, paths, visited,
							)?;
							entry.map(Either::Left)
						},
						Some(Either::Right(object)) => Some(Either::Right(object.clone())),
						None => return Err(tg::error!("invalid lockfile")),
					};
					entries_.insert(name.clone(), entry);
				}

				// Create a new node.
				let new_node = nodes.len();
				visited.insert(node, Some(new_node));
				nodes.push(tg::lockfile::Node::Directory { entries: entries_ });

				// Add to the paths if this is a package directory.
				if let Some(path) = old_paths.get(&node) {
					let is_package_dir = entries.keys().any(|name| {
						tg::package::ROOT_MODULE_FILE_NAMES
							.iter()
							.any(|root| *root == name)
					});
					if is_package_dir {
						paths.entry((*path).clone()).or_default().push(new_node);
					}
				}

				Ok(Some(new_node))
			},
			tg::lockfile::Node::File {
				dependencies,
				executable,
				..
			} => {
				if dependencies.is_empty() {
					visited.insert(node, None);
					Ok(None)
				} else {
					// Create the node.
					let new_node = nodes.len();
					visited.insert(node, Some(new_node));
					nodes.push(tg::lockfile::Node::File {
						contents: None,
						dependencies: BTreeMap::new(),
						executable: *executable,
					});

					// Recurse and collect new dependencies.
					let dependencies_ = dependencies
						.iter()
						.map(|(reference, dependency)| {
							let object = match &dependency.object {
								Some(Either::Left(node)) => {
									let entry = self.strip_lockfile_inner(
										lockfile, old_paths, *node, nodes, paths, visited,
									)?;
									entry.map(Either::Left)
								},
								Some(Either::Right(object)) => Some(Either::Right(object.clone())),
								None => return Err(tg::error!("invalid lockfile")),
							};
							let dependency = tg::lockfile::Dependency {
								object,
								tag: dependency.tag.clone(),
							};
							Ok::<_, tg::Error>((reference.clone(), dependency))
						})
						.try_collect()?;

					// Update dependencies.
					let tg::lockfile::Node::File { dependencies, .. } = &mut nodes[new_node] else {
						unreachable!()
					};
					*dependencies = dependencies_;

					Ok(Some(new_node))
				}
			},

			tg::lockfile::Node::Symlink { artifact, path } => {
				// Remap the artifact if necessary.
				let artifact = artifact
					.as_ref()
					.ok_or_else(|| tg::error!("invalid lockfile"))?;
				let artifact = match artifact {
					Some(Either::Left(node)) => {
						let entry = self.strip_lockfile_inner(
							lockfile, old_paths, *node, nodes, paths, visited,
						)?;
						entry.map(|index| Some(Either::Left(index)))
					},
					Some(Either::Right(object)) => Some(Some(Either::Right(object.clone()))),
					None => Some(None),
				};

				// Create the node.
				let new_node = nodes.len();
				nodes.push(tg::lockfile::Node::Symlink {
					artifact,
					path: path.clone(),
				});
				visited.insert(node, Some(new_node));

				Ok(Some(new_node))
			},
		}
	}
}
