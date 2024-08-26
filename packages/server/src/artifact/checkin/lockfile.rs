use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt,
	sync::{Arc, RwLock},
};

use crate::Server;
use itertools::Itertools;
use tangram_client as tg;
use tangram_either::Either;

use super::{
	input::Input,
	unify::{Graph, Id},
};

impl Server {
	pub(super) async fn create_lockfile(
		&self,
		graph: &Graph,
		root: &Id,
	) -> tg::Result<tg::Lockfile> {
		let root_path = graph
			.nodes
			.get(root)
			.unwrap()
			.object
			.as_ref()
			.unwrap_left()
			.read()
			.unwrap()
			.arg
			.path
			.clone();

		let mut paths = BTreeMap::new();
		let mut lockfile_nodes = Vec::with_capacity(graph.nodes.len());
		let mut visited = BTreeMap::new();
		self.create_lockfile_inner(
			&root_path,
			graph,
			root,
			&mut lockfile_nodes,
			&mut visited,
			&mut paths,
		)
		.await?
		.left()
		.ok_or_else(|| tg::error!("expected a the root to have an index"))?;
		let nodes = lockfile_nodes.into_iter().map(Option::unwrap).collect();
		Ok(tg::Lockfile { paths, nodes })
	}

	async fn create_lockfile_inner(
		&self,
		root: &tg::Path,
		graph: &Graph,
		id: &Id,
		lockfile_nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<Id, Either<usize, tg::object::Id>>,
		paths: &mut BTreeMap<tg::Path, usize>,
	) -> tg::Result<Either<usize, tg::object::Id>> {
		// Check if we've visited this node already.
		if let Some(either) = visited.get(id) {
			return Ok(either.clone());
		}

		// Get the graph node.
		let graph_node = graph.nodes.get(id).unwrap();

		// If this is an object inline it.
		let input = match &graph_node.object {
			Either::Left(input) => input.clone(),
			Either::Right(object) => {
				let lockfile_id = Either::Right(object.clone());
				visited.insert(id.clone(), lockfile_id.clone());
				return Ok(lockfile_id);
			},
		};

		// Get the artifact kind.
		let kind = match &graph_node.object {
			// If this is an artifact on disk,
			Either::Left(object) => {
				let input = object.read().unwrap();
				if input.metadata.is_dir() {
					tg::artifact::Kind::Directory
				} else if input.metadata.is_file() {
					tg::artifact::Kind::File
				} else {
					tg::artifact::Kind::Symlink
				}
			},

			// If this is an object without any dependencies, inline it.
			Either::Right(object) if graph_node.outgoing.is_empty() => {
				let either = Either::Right(object.clone());
				visited.insert(id.clone(), either.clone());
				return Ok(either);
			},

			// Otherwise, create a node.
			Either::Right(tg::object::Id::Directory(_)) => tg::artifact::Kind::Directory,
			Either::Right(tg::object::Id::File(_)) => tg::artifact::Kind::File,
			Either::Right(_) => return Err(tg::error!("invalid input graph")),
		};

		// Get the index of this node.
		let index = lockfile_nodes.len();
		lockfile_nodes.push(None);

		// Update the visited table.
		visited.insert(id.clone(), Either::Left(index));

		// Update the paths table.
		let path = input.read().unwrap().arg.path.clone();
		let path = path.diff(root).unwrap();
		paths.insert(path, index);

		// Recurse over dependencies.
		let mut dependencies = BTreeMap::new();
		for (reference, input_graph_id) in &graph_node.outgoing {
			let output_graph_id = Box::pin(self.create_lockfile_inner(
				root,
				graph,
				input_graph_id,
				lockfile_nodes,
				visited,
				paths,
			))
			.await?;
			dependencies.insert(reference.clone(), output_graph_id);
		}

		// Create the node.
		let output_node = match kind {
			tg::artifact::Kind::Directory => {
				let entries = dependencies
					.into_iter()
					.map(|(reference, id)| {
						let name = reference
							.path()
							.try_unwrap_path_ref()
							.map_err(|_| tg::error!("invalid input graph"))?
							.components()
							.last()
							.ok_or_else(|| tg::error!("invalid input graph"))?
							.try_unwrap_normal_ref()
							.map_err(|_| tg::error!("invalid input graph"))?
							.clone();
						let id = match id {
							Either::Left(id) => Either::Left(id),
							Either::Right(tg::object::Id::Directory(id)) => {
								Either::Right(id.into())
							},
							Either::Right(tg::object::Id::File(id)) => Either::Right(id.into()),
							Either::Right(tg::object::Id::Symlink(id)) => Either::Right(id.into()),
							Either::Right(_) => return Err(tg::error!("invalid input graph")),
						};
						Ok::<_, tg::Error>((name, Some(id)))
					})
					.try_collect()?;
				tg::lockfile::Node::Directory { entries }
			},
			tg::artifact::Kind::File => {
				let input = input.read().unwrap().clone();
				self.create_file_data(input, dependencies).await?
			},
			tg::artifact::Kind::Symlink => {
				let input = input.read().unwrap().clone();
				self.create_symlink_data(input).await?
			},
		};

		// Update the node and return the index.
		lockfile_nodes[index].replace(output_node);
		Ok(Either::Left(index))
	}

	async fn create_file_data(
		&self,
		input: Input,
		dependencies: BTreeMap<tg::Reference, Either<usize, tg::object::Id>>,
	) -> tg::Result<tg::lockfile::Node> {
		let super::input::Input { arg, metadata, .. } = input;

		// Create the blob.
		let file = tokio::fs::File::open(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read file"))?;
		let output = self
			.create_blob(file)
			.await
			.map_err(|source| tg::error!(!source, "failed to create blob"))?;
		let dependencies = dependencies
			.into_iter()
			.map(|(k, v)| (k, Some(v)))
			.collect::<BTreeMap<_, _>>();

		if !dependencies.is_empty() && input.dependencies.is_none() {
			return Err(tg::error!("invalid input"));
		}

		let dependencies = input.dependencies.is_some().then_some(dependencies);

		// Create the data.
		Ok(tg::lockfile::Node::File {
			contents: Some(output.blob),
			dependencies,
			executable: metadata.permissions().mode() & 0o111 != 0,
		})
	}

	async fn create_symlink_data(&self, input: Input) -> tg::Result<tg::lockfile::Node> {
		let Input { arg, .. } = input;
		let path = arg.path;

		// Read the target from the symlink.
		let target = tokio::fs::read_link(&path).await.map_err(
			|source| tg::error!(!source, %path, r#"failed to read the symlink at path"#,),
		)?;

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
}

impl Server {
	pub(super) async fn write_lockfiles(
		&self,
		input: Arc<RwLock<Input>>,
		lockfile: &tg::Lockfile,
	) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		let root = input.read().unwrap().arg.path.clone();
		self.write_lockfiles_inner(&root, input, lockfile, &mut visited)
			.await
	}

	pub(super) async fn write_lockfiles_inner(
		&self,
		root: &tg::Path,
		input: Arc<RwLock<Input>>,
		lockfile: &tg::Lockfile,
		visited: &mut BTreeSet<tg::Path>,
	) -> tg::Result<()> {
		// Handle the lockfile if needed.
		'a: {
			let Some((_, root_module_path)) = input.read().unwrap().lockfile.clone() else {
				break 'a;
			};
			if visited.contains(&root_module_path) {
				break 'a;
			}
			visited.insert(root_module_path.clone());
			let diff = root_module_path.diff(root).unwrap();

			let root = *lockfile
				.paths
				.get(&diff)
				.ok_or_else(|| tg::error!("invalid lockfile"))?;

			// Strip the lockfile.
			let stripped_lockfile = self.strip_lockfile(lockfile, root).await?;
			let contents = serde_json::to_string_pretty(&stripped_lockfile)
				.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let lockfile_path = root_module_path
				.parent()
				.join(tg::lockfile::TANGRAM_LOCKFILE_FILE_NAME)
				.normalize();

			tokio::fs::write(&lockfile_path, &contents)
				.await
				.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
		}
		// Recurse.
		let children = input
			.read()
			.unwrap()
			.dependencies
			.iter()
			.flat_map(|map| map.values().flatten())
			.cloned()
			.filter_map(Either::right)
			.collect::<Vec<_>>();
		for child in children {
			Box::pin(self.write_lockfiles_inner(root, child, lockfile, visited)).await?;
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
			.map(|(path, node)| (*node, path))
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
		old_paths: &BTreeMap<usize, &tg::Path>,
		node: usize,
		nodes: &mut Vec<tg::lockfile::Node>,
		paths: &mut BTreeMap<tg::Path, usize>,
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

				// Add the path.
				if let Some(path) = old_paths.get(&node) {
					paths.insert((*path).clone(), new_node);
				}

				Ok(Some(new_node))
			},
			tg::lockfile::Node::File {
				dependencies,
				executable,
				..
			} => {
				if let Some(dependencies) = dependencies {
					// Create the node.
					let new_node = nodes.len();
					visited.insert(node, Some(new_node));
					nodes.push(tg::lockfile::Node::File {
						contents: None,
						dependencies: None,
						executable: *executable,
					});

					// Add the path.
					if let Some(path) = old_paths.get(&node) {
						paths.insert((*path).clone(), new_node);
					}

					// Recurse and collect new dependencies.
					let mut dependencies_ = BTreeMap::new();
					for (reference, entry) in dependencies {
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
						dependencies_.insert(reference.clone(), entry);
					}

					// Update dependencies.
					let tg::lockfile::Node::File { dependencies, .. } = &mut nodes[new_node] else {
						unreachable!()
					};
					dependencies.replace(dependencies_);

					Ok(Some(new_node))
				} else {
					visited.insert(node, None);
					Ok(None)
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

				// Add the path.
				if let Some(path) = old_paths.get(&node) {
					paths.insert((*path).clone(), new_node);
				}

				Ok(Some(new_node))
			},
		}
	}
}
