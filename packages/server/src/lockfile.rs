use crate::Server;
use itertools::Itertools;
use std::{
	collections::{BTreeMap, HashMap},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_either::Either;

#[derive(Clone, Debug)]
pub struct Lockfile {
	nodes: Vec<tg::lockfile::Node>,
	paths: Vec<Option<PathBuf>>,
}

#[derive(Clone)]
struct FindInLockfileArg<'a> {
	current_node_path: PathBuf,
	current_node: usize,
	current_package_node: usize,
	current_package_path: PathBuf,
	nodes: &'a [tg::lockfile::Node],
	search: Either<usize, &'a Path>,
}

pub struct LockfileNode {
	pub node: usize,
}

impl Server {
	pub(crate) fn create_lockfile_for_artifact(
		&self,
		artifact: &tg::artifact::Id,
		checkout_dependencies: bool,
	) -> tg::Result<tg::Lockfile> {
		// Create the state.
		let mut nodes = Vec::new();
		let mut visited = HashMap::default();
		let mut graphs = HashMap::default();

		// Create nodes in the lockfile for the graph.
		let root = self.get_or_create_lockfile_node_for_artifact(
			artifact.clone(),
			checkout_dependencies,
			&mut nodes,
			&mut visited,
			&mut graphs,
		)?;
		let nodes: Vec<_> = nodes
			.into_iter()
			.enumerate()
			.map(|(index, node)| {
				node.ok_or_else(
					|| tg::error!(%node = index, "invalid graph, failed to create lockfile node"),
				)
			})
			.collect::<tg::Result<_>>()?;

		// Strip nodes.
		let nodes = Self::strip_lockfile_nodes(&nodes, &vec![false; nodes.len()], root)?;

		// Create the lockfile.
		let lockfile = tg::Lockfile { nodes };

		Ok(lockfile)
	}

	fn get_or_create_lockfile_node_for_artifact(
		&self,
		id: tg::artifact::Id,
		checkout_dependencies: bool,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut HashMap<tg::artifact::Id, usize, fnv::FnvBuildHasher>,
		graphs: &mut HashMap<tg::graph::Id, Vec<usize>, fnv::FnvBuildHasher>,
	) -> tg::Result<usize> {
		if let Some(visited) = visited.get(&id) {
			return Ok(*visited);
		}
		let index = nodes.len();

		#[allow(clippy::match_wildcard_for_single_variants)]
		let artifact = match &self.store {
			crate::store::Store::Lmdb(lmdb) => lmdb.try_get_object_data_sync(&id.clone().into())?,
			crate::store::Store::Memory(memory) => {
				memory.try_get_object_data(&id.clone().into())?
			},
			_ => {
				return Err(tg::error!("invalid store"));
			},
		}
		.ok_or_else(|| tg::error!("expected the object to exist"))?
		.try_into()
		.map_err(|_| tg::error!("expected an artifact"))?;

		// Flatten graphs into the lockfile.
		'a: {
			let (graph, node) = match &artifact {
				tg::artifact::Data::Directory(directory) => {
					let tg::directory::Data::Graph { graph, node } = directory else {
						break 'a;
					};
					(graph, node)
				},
				tg::artifact::Data::File(file) => {
					let tg::file::Data::Graph { graph, node } = file else {
						break 'a;
					};
					(graph, node)
				},
				tg::artifact::Data::Symlink(symlink) => {
					let tg::symlink::Data::Graph { graph, node } = symlink else {
						break 'a;
					};
					(graph, node)
				},
			};
			let nodes = self.create_lockfile_node_with_graph(
				checkout_dependencies,
				graph,
				nodes,
				visited,
				graphs,
			)?;
			return Ok(nodes[*node]);
		}

		// Only create a distinct node for non-graph artifacts.
		nodes.push(None);
		visited.insert(id, index);

		// Create a new lockfile node for the artifact, recursing over dependencies.
		let node = match &artifact {
			tg::artifact::Data::Directory(directory) => {
				let tg::directory::data::Directory::Normal { entries } = directory else {
					unreachable!()
				};
				let mut entries_ = BTreeMap::new();
				for (name, artifact) in entries {
					let index = self.get_or_create_lockfile_node_for_artifact(
						artifact.clone(),
						checkout_dependencies,
						nodes,
						visited,
						graphs,
					)?;
					entries_.insert(name.clone(), Either::Left(index));
				}
				tg::lockfile::Node::Directory(tg::lockfile::Directory { entries: entries_ })
			},

			tg::artifact::Data::File(file) => {
				let tg::file::data::File::Normal {
					contents,
					dependencies,
					executable,
				} = file
				else {
					unreachable!()
				};
				let mut dependencies_ = BTreeMap::new();
				for (reference, referent) in dependencies {
					let item = match &referent.item {
						tg::object::Id::Directory(id) if checkout_dependencies => {
							let index = self.get_or_create_lockfile_node_for_artifact(
								id.clone().into(),
								checkout_dependencies,
								nodes,
								visited,
								graphs,
							)?;
							Either::Left(index)
						},
						tg::object::Id::File(id) if checkout_dependencies => {
							let index = self.get_or_create_lockfile_node_for_artifact(
								id.clone().into(),
								checkout_dependencies,
								nodes,
								visited,
								graphs,
							)?;
							Either::Left(index)
						},
						tg::object::Id::Symlink(id) if checkout_dependencies => {
							let index = self.get_or_create_lockfile_node_for_artifact(
								id.clone().into(),
								checkout_dependencies,
								nodes,
								visited,
								graphs,
							)?;
							Either::Left(index)
						},
						object => Either::Right(object.clone()),
					};
					let dependency = tg::Referent {
						item,
						path: referent.path.clone(),
						tag: referent.tag.clone(),
					};
					dependencies_.insert(reference.clone(), dependency);
				}
				let contents = Some(contents.clone());
				tg::lockfile::Node::File(tg::lockfile::File {
					contents,
					dependencies: dependencies_,
					executable: *executable,
				})
			},

			tg::artifact::Data::Symlink(symlink) => match symlink {
				tg::symlink::data::Symlink::Graph { .. } => unreachable!(),
				tg::symlink::data::Symlink::Target { target } => {
					tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target {
						target: target.clone(),
					})
				},
				tg::symlink::data::Symlink::Artifact { artifact, subpath } => {
					let artifact = {
						let index = self.get_or_create_lockfile_node_for_artifact(
							artifact.clone(),
							checkout_dependencies,
							nodes,
							visited,
							graphs,
						)?;
						Either::Left(index)
					};
					let subpath = subpath.as_ref().map(PathBuf::from);
					tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
						artifact,
						subpath,
					})
				},
			},
		};

		// Update the visited set.
		nodes[index].replace(node);

		Ok(index)
	}

	fn create_lockfile_node_with_graph(
		&self,
		checkout_dependencies: bool,
		id: &tg::graph::Id,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut HashMap<tg::artifact::Id, usize, fnv::FnvBuildHasher>,
		graphs: &mut HashMap<tg::graph::Id, Vec<usize>, fnv::FnvBuildHasher>,
	) -> tg::Result<Vec<usize>> {
		if let Some(existing) = graphs.get(id) {
			return Ok(existing.clone());
		}
		#[allow(clippy::match_wildcard_for_single_variants)]
		let graph: tg::graph::Data = match &self.store {
			crate::store::Store::Lmdb(lmdb) => lmdb.try_get_object_data_sync(&id.clone().into())?,
			crate::store::Store::Memory(memory) => {
				memory.try_get_object_data(&id.clone().into())?
			},
			_ => {
				return Err(tg::error!("invalid store"));
			},
		}
		.ok_or_else(|| tg::error!("expected the graph to exist"))?
		.try_into()
		.map_err(|_| tg::error!("expected a graph"))?;

		// Assign indices.
		let mut indices = Vec::with_capacity(graph.nodes.len());
		for node in 0..graph.nodes.len() {
			let kind = graph.nodes[node].kind();
			let data: tg::artifact::Data = match kind {
				tg::artifact::Kind::Directory => tg::directory::Data::Graph {
					graph: id.clone(),
					node,
				}
				.into(),
				tg::artifact::Kind::File => tg::file::Data::Graph {
					graph: id.clone(),
					node,
				}
				.into(),
				tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
					graph: id.clone(),
					node,
				}
				.into(),
			};
			let bytes = data.serialize()?;
			let id = tg::artifact::Id::new(kind, &bytes);

			let index = visited.get(&id).copied().unwrap_or_else(|| {
				let index = nodes.len();
				visited.insert(id.clone(), index);
				nodes.push(None);
				index
			});
			indices.push(index);
		}
		graphs.insert(id.clone(), indices.clone());

		// Create nodes
		for (old_index, node) in graph.nodes.iter().enumerate() {
			let node = match node {
				tg::graph::data::Node::Directory(directory) => {
					let mut entries = BTreeMap::new();
					for (name, entry) in &directory.entries {
						let index = match entry {
							Either::Left(index) => indices[*index],
							Either::Right(artifact) => self
								.get_or_create_lockfile_node_for_artifact(
									artifact.clone(),
									checkout_dependencies,
									nodes,
									visited,
									graphs,
								)?,
						};
						entries.insert(name.clone(), Either::Left(index));
					}
					tg::lockfile::Node::Directory(tg::lockfile::Directory { entries })
				},

				tg::graph::data::Node::File(file) => {
					let mut dependencies = BTreeMap::new();
					for (reference, referent) in &file.dependencies {
						let item = match &referent.item {
							Either::Left(index) => Either::Left(indices[*index]),
							Either::Right(object) => match object {
								tg::object::Id::Directory(id) => {
									let index = self.get_or_create_lockfile_node_for_artifact(
										id.clone().into(),
										checkout_dependencies,
										nodes,
										visited,
										graphs,
									)?;
									Either::Left(index)
								},
								tg::object::Id::File(id) => {
									let index = self.get_or_create_lockfile_node_for_artifact(
										id.clone().into(),
										checkout_dependencies,
										nodes,
										visited,
										graphs,
									)?;
									Either::Left(index)
								},
								tg::object::Id::Symlink(id) => {
									let index = self.get_or_create_lockfile_node_for_artifact(
										id.clone().into(),
										checkout_dependencies,
										nodes,
										visited,
										graphs,
									)?;
									Either::Left(index)
								},
								object => Either::Right(object.clone()),
							},
						};
						let path = referent.path.clone();
						let tag = referent.tag.clone();
						let dependency = tg::Referent { item, path, tag };
						dependencies.insert(reference.clone(), dependency);
					}
					let contents = file.contents.clone();
					let executable = file.executable;
					tg::lockfile::Node::File(tg::lockfile::File {
						contents: Some(contents),
						dependencies,
						executable,
					})
				},

				tg::graph::data::Node::Symlink(symlink) => match symlink {
					tg::graph::data::Symlink::Target { target } => {
						tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target {
							target: target.clone(),
						})
					},
					tg::graph::data::Symlink::Artifact { artifact, subpath } => {
						let artifact = match artifact {
							Either::Left(index) => indices[*index],
							Either::Right(artifact) => self
								.get_or_create_lockfile_node_for_artifact(
									artifact.clone(),
									checkout_dependencies,
									nodes,
									visited,
									graphs,
								)?,
						};
						let artifact = Either::Left(artifact);
						tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
							artifact,
							subpath: subpath.clone(),
						})
					},
				},
			};

			let index = indices[old_index];
			nodes[index].replace(node);
		}

		Ok(indices)
	}

	pub(crate) async fn find_node_in_lockfile(
		&self,
		search: Either<usize, &Path>,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<LockfileNode> {
		self.find_node_in_lockfile_nodes(search, lockfile_path, &lockfile.nodes)
			.await
	}

	pub(crate) async fn find_node_in_lockfile_nodes(
		&self,
		search: Either<usize, &Path>,
		lockfile_path: &Path,
		nodes: &[tg::lockfile::Node],
	) -> tg::Result<LockfileNode> {
		let current_package_path = lockfile_path.parent().unwrap().to_owned();
		let current_package_node = 0;
		if nodes.is_empty() {
			return Err(tg::error!("invalid lockfile"));
		}
		let mut visited = vec![false; nodes.len()];

		let arg = FindInLockfileArg {
			current_node_path: current_package_path.clone(),
			current_node: current_package_node,
			current_package_path,
			current_package_node,
			nodes,
			search,
		};

		self.find_node_in_lockfile_inner(arg.clone(), &mut visited)
			.await?
			.ok_or_else(
				|| tg::error!(%lockfile = lockfile_path.display(), ?search = arg.search, "failed to find node in lockfile"),
			)
	}

	pub(crate) async fn find_node_index_in_lockfile(
		&self,
		path: &Path,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<usize> {
		let result = self
			.find_node_in_lockfile(Either::Right(path), lockfile_path, lockfile)
			.await?;
		Ok(result.node)
	}

	async fn find_node_in_lockfile_inner(
		&self,
		mut arg: FindInLockfileArg<'_>,
		visited: &mut [bool],
	) -> tg::Result<Option<LockfileNode>> {
		// If this is the node we're searching for, return.
		match arg.search {
			Either::Left(node) if node == arg.current_node => {
				let result = LockfileNode {
					node: arg.current_node,
				};
				return Ok(Some(result));
			},
			Either::Right(path) if path == arg.current_node_path => {
				let result = LockfileNode {
					node: arg.current_node,
				};
				return Ok(Some(result));
			},
			_ => (),
		}

		// Check if this node has been visited and update the visited set.
		if visited[arg.current_node] {
			return Ok(None);
		}
		visited[arg.current_node] = true;

		match &arg.nodes[arg.current_node] {
			tg::lockfile::Node::Directory(tg::lockfile::Directory { entries, .. }) => {
				// If this is a directory with a root module, update the current package path/node.
				if entries
					.keys()
					.any(|name| tg::package::is_root_module_path(name.as_ref()))
				{
					arg.current_package_path = arg.current_node_path.clone();
					arg.current_package_node = arg.current_node;
				}

				// Recurse over the entries.
				for (name, entry) in entries {
					let current_node_path = arg.current_node_path.join(name);
					let current_node = *entry.as_ref().unwrap_left();

					let arg = FindInLockfileArg {
						current_node_path,
						current_node,
						..arg.clone()
					};
					let result = Box::pin(self.find_node_in_lockfile_inner(arg, visited)).await?;
					if let Some(result) = result {
						return Ok(Some(result));
					}
				}
			},

			tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) => {
				for (reference, dependency) in dependencies {
					// Skip dependencies that are not contained in the lockfile.
					let Some(dependency_package_node) = dependency.item.as_ref().left().copied()
					else {
						continue;
					};

					// Skip dependencies contained within the same package, since the traversal is guaranteed to reach them.
					if dependency_package_node == arg.current_package_node {
						continue;
					}

					// Compute the canonical path of the import.
					let path = reference
						.item()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| reference.options()?.path.as_ref())
						.ok_or_else(|| tg::error!(%reference, "expected a path reference"))?;
					let path = arg.current_node_path.parent().unwrap().join(path);
					let path = tokio::fs::canonicalize(&path).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path"),
					)?;

					let current_package_node = dependency_package_node;
					let current_package_path = path.clone();

					// Recurse on the dependency's package.
					let arg = FindInLockfileArg {
						current_node: current_package_node,
						current_node_path: current_package_path.clone(),
						current_package_node,
						current_package_path,
						..arg.clone()
					};
					let result = Box::pin(self.find_node_in_lockfile_inner(arg, visited)).await?;

					if let Some(path) = result {
						return Ok(Some(path));
					}
				}
			},

			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { .. }) => {
				return Ok(None);
			},

			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
				artifact,
				subpath,
				..
			}) => {
				// Get the referent artifact.
				let Either::Left(artifact) = artifact else {
					return Ok(None);
				};

				// If the artifact is in the same package, skip it because it will eventually be discovered.
				if *artifact == arg.current_package_node {
					return Ok(None);
				}

				// Compute the canonical path of the target.
				let target = tokio::fs::read_link(&arg.current_node_path).await.map_err(
					|source| tg::error!(!source, %path = arg.current_node_path.display(), "failed to readlink"),
				)?;
				let path = arg.current_node_path.join(target);
				let path = tokio::fs::canonicalize(&path).await.map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path"),
				)?;

				// Strip the subpath.
				let subpath = subpath.as_deref().unwrap_or("".as_ref());
				let current_package_path = strip_subpath(&path, subpath)?;
				let current_package_node = *artifact;

				// Recurse on the package.
				let arg = FindInLockfileArg {
					current_node: current_package_node,
					current_node_path: current_package_path.clone(),
					current_package_node,
					current_package_path,
					..arg.clone()
				};
				let result = Box::pin(self.find_node_in_lockfile_inner(arg, visited)).await?;
				if let Some(result) = result {
					return Ok(Some(result));
				}
			},
		}

		Ok(None)
	}
}

// Remove a subpath from a base path.
fn strip_subpath(base: &Path, subpath: &Path) -> tg::Result<PathBuf> {
	if subpath.is_absolute() {
		return Err(tg::error!("invalid subpath"));
	}
	let subpath = subpath.strip_prefix("./").unwrap_or(subpath.as_ref());
	if !base.ends_with(subpath) {
		return Err(
			tg::error!(%base = base.display(), %subpath = subpath.display(), "cannot remove subpath from base"),
		);
	}
	let path = base
		.components()
		.take(base.components().count() - subpath.components().count())
		.collect();
	Ok(path)
}

pub type ResolvedDependency = Option<Either<PathBuf, tg::object::Id>>;
impl Lockfile {
	pub fn try_resolve_dependency(
		&self,
		node_path: &Path,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::Referent<ResolvedDependency>>> {
		let node = self.get_node_for_path(node_path)?;

		// Dependency resolution is only valid for files.
		let tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) = &self.nodes[node]
		else {
			return Err(tg::error!(%path = node_path.display(), "expected a file node"))?;
		};

		// Lookup the dependency.
		let Some(referent) = dependencies.get(reference) else {
			return Ok(None);
		};

		// Resolve the item.
		let item = match &referent.item {
			Either::Left(index) => {
				if let Some(path) = self.paths[*index].clone() {
					Some(Either::Left(path))
				} else if referent.tag.is_some() {
					None
				} else {
					return Ok(None);
				}
			},
			Either::Right(object) => Some(Either::Right(object.clone())),
		};

		// Construct the referent.
		let referent = tg::Referent {
			item,
			tag: referent.tag.clone(),
			path: referent.path.clone(),
		};

		Ok(Some(referent))
	}

	pub fn get_path_for_node(&self, node: usize) -> tg::Result<PathBuf> {
		self.paths[node]
			.clone()
			.ok_or_else(|| tg::error!("expected a node path"))
	}

	pub fn get_node_for_path(&self, node_path: &Path) -> tg::Result<usize> {
		// Linear search, which should be faster than looking in a btreemap. Replace with a btreemap if this is too slow.
		self.paths
			.iter()
			.position(|path| path.as_deref() == Some(node_path))
			.ok_or_else(|| tg::error!(%path = node_path.display(), "failed to find in lockfile"))
	}

	pub fn get_file_dependencies(
		&self,
		path: &Path,
	) -> tg::Result<Vec<(tg::Reference, tg::Referent<ResolvedDependency>)>> {
		// Find the node index of this path.
		let node = self.get_node_for_path(path)?;

		// Get the corresponding file node.
		let tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) = &self.nodes[node]
		else {
			return Err(tg::error!(%path = path.display(), "expected a file"));
		};

		// Resolve the dependencies.
		let mut dependencies_ = Vec::with_capacity(dependencies.len());
		for (reference, referent) in dependencies {
			// Resolve the item.
			let item = match &referent.item {
				Either::Left(index) => self.paths[*index].clone().map(Either::Left),
				Either::Right(object) => Some(Either::Right(object.clone())),
			};
			let referent = tg::Referent {
				item,
				path: referent.path.clone(),
				tag: referent.tag.clone(),
			};
			dependencies_.push((reference.clone(), referent));
		}

		Ok(dependencies_)
	}
}

impl Server {
	pub fn try_parse_lockfile(&self, path: &Path) -> tg::Result<Option<Lockfile>> {
		let contents = 'a: {
			// Read the lockfile's xattrs.
			let Ok(Some(contents)) = xattr::get(path, tg::file::XATTR_LOCK_NAME) else {
				break 'a None;
			};
			Some(contents)
		};

		// If not available in the xattrs, try and read the file.
		let contents = 'a: {
			if let Some(contents) = contents {
				break 'a Some(contents);
			}

			// Read the lockfile from disk.
			let lockfile_path = path.join(tg::package::LOCKFILE_FILE_NAME);
			let contents = match std::fs::read(&lockfile_path) {
				Ok(contents) => contents,
				Err(error)
					if matches!(
						error.kind(),
						std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
					) =>
				{
					break 'a None;
				},
				Err(source) => {
					return Err(
						tg::error!(!source, %path = lockfile_path.display(), "failed to read lockfile"),
					);
				},
			};
			Some(contents)
		};

		let Some(contents) = contents else {
			return Ok(None);
		};

		// Deserialize the lockfile.
		let lockfile = serde_json::from_slice::<tg::Lockfile>(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize lockfile"),
		)?;

		// Get any referenced objects that are not contained within the lockfile.
		let mut object_ids: Vec<tg::object::Id> = Vec::new();
		for node in &lockfile.nodes {
			match node {
				tg::lockfile::Node::Directory(tg::lockfile::Directory { entries, .. }) => {
					let it = entries
						.values()
						.filter_map(|value| value.as_ref().right().cloned());
					object_ids.extend(it);
				},
				tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) => {
					let it =
						dependencies
							.values()
							.filter_map(|referent| match referent.item.clone() {
								Either::Right(tg::object::Id::Directory(id)) => Some(id.into()),
								Either::Right(tg::object::Id::File(id)) => Some(id.into()),
								Either::Right(tg::object::Id::Symlink(id)) => Some(id.into()),
								_ => None,
							});
					object_ids.extend(it);
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
					artifact, ..
				}) => {
					let it = artifact.as_ref().right().cloned();
					object_ids.extend(it);
				},
				tg::lockfile::Node::Symlink(_) => (),
			}
		}
		let artifact_ids: Vec<tg::artifact::Id> = object_ids
			.into_iter()
			.map(tg::artifact::Id::try_from)
			.try_collect()?;

		// Get the artifacts path.
		let mut artifacts_path = None;
		for path in path.ancestors().skip(1) {
			let path = path.join(".tangram/artifacts");
			if matches!(path.try_exists(), Ok(true)) {
				artifacts_path.replace(path);
				break;
			}
		}
		let artifacts_path = artifacts_path.unwrap_or_else(|| self.artifacts_path());

		// Find paths for any artifacts.
		let mut artifacts = BTreeMap::new();
		for id in artifact_ids {
			// Check if the file exists.
			let path = artifacts_path.join(id.to_string());
			if matches!(path.try_exists(), Ok(true)) {
				artifacts.insert(id, path);
			}
		}
		// Get the paths for the lockfile nodes.
		let paths = get_paths(path, &lockfile)?;

		// Create the parsed lockfile.
		let lockfile = Lockfile {
			nodes: lockfile.nodes,
			paths,
		};

		Ok(Some(lockfile))
	}
}

impl Server {
	pub async fn create_object_from_lockfile_node(
		&self,
		lockfile: &tg::Lockfile,
		node: usize,
	) -> tg::Result<tg::Object> {
		let mut visited = vec![None; lockfile.nodes.len()];

		// Create graphs.
		let sccs = petgraph::algo::tarjan_scc(LockfileGraph(&lockfile.nodes));
		for mut scc in sccs {
			// Skip any graphs that we can't construct.
			let skip = scc
				.iter()
				.copied()
				.filter_map(|index| lockfile.nodes[index].try_unwrap_file_ref().ok())
				.any(|file| file.contents.is_none());
			if skip {
				continue;
			}

			// Create normal objects for existing items.
			if scc.len() == 1 {
				Self::create_normal_object_from_lockfile_node(lockfile, scc[0], &mut visited)?;
				continue;
			}
			scc.reverse();
			let graph_indices = scc
				.iter()
				.copied()
				.enumerate()
				.map(|(graph, lockfile)| (lockfile, graph))
				.collect::<BTreeMap<_, _>>();

			// Create the graph object.
			let mut nodes = Vec::with_capacity(scc.len());
			for lockfile_index in &scc {
				self.create_graph_node_from_lockfile_node(
					lockfile,
					*lockfile_index,
					&graph_indices,
					&mut nodes,
					&mut visited,
				)?;
			}

			// Construct the graph.
			let graph = tg::Graph::with_nodes(nodes);

			// Construct the objects.
			for (graph_index, lockfile_index) in scc.into_iter().enumerate() {
				let object = match &lockfile.nodes[lockfile_index] {
					tg::lockfile::Node::Directory(_) => {
						tg::Directory::with_graph_and_node(graph.clone(), graph_index).into()
					},
					tg::lockfile::Node::File(_) => {
						tg::File::with_graph_and_node(graph.clone(), graph_index).into()
					},
					tg::lockfile::Node::Symlink(_) => {
						tg::Symlink::with_graph_and_node(graph.clone(), graph_index).into()
					},
				};
				visited[lockfile_index].replace(object);
			}
		}
		visited[node]
			.clone()
			.ok_or_else(|| tg::error!("failed to create the object"))
	}

	pub fn create_graph_node_from_lockfile_node(
		&self,
		lockfile: &tg::Lockfile,
		lockfile_index: usize,
		graph_indices: &BTreeMap<usize, usize>,
		nodes: &mut Vec<tg::graph::Node>,
		visited: &mut [Option<tg::Object>],
	) -> tg::Result<()> {
		let node = match &lockfile.nodes[lockfile_index] {
			tg::lockfile::Node::Directory(directory) => {
				let entries = directory
					.entries
					.clone()
					.into_iter()
					.map(|(name, entry)| {
						let entry = match entry {
							Either::Left(lockfile_index)
								if graph_indices.contains_key(&lockfile_index) =>
							{
								Either::Left(graph_indices.get(&lockfile_index).copied().unwrap())
							},
							Either::Left(lockfile_index) => {
								let object = visited[lockfile_index]
									.as_ref()
									.ok_or_else(|| tg::error!("expected an object"))?
									.clone()
									.try_into()?;
								Either::Right(object)
							},
							Either::Right(id) => {
								Either::Right(tg::Artifact::with_id(id.clone().try_into()?))
							},
						};
						Ok::<_, tg::Error>((name, entry))
					})
					.try_collect()?;
				let directory = tg::graph::object::Directory { entries };
				tg::graph::Node::Directory(directory)
			},
			tg::lockfile::Node::File(file) => {
				let contents = file
					.contents
					.clone()
					.ok_or_else(|| tg::error!("expected file contents"))?;
				let dependencies = file
					.dependencies
					.clone()
					.into_iter()
					.map(|(reference, referent)| {
						let tg::Referent { item, path, tag } = referent;
						let item = match item {
							Either::Left(lockfile_index)
								if graph_indices.contains_key(&lockfile_index) =>
							{
								Either::Left(graph_indices.get(&lockfile_index).copied().unwrap())
							},
							Either::Left(lockfile_index) => {
								let object = visited[lockfile_index]
									.as_ref()
									.ok_or_else(|| tg::error!("expected an object"))?
									.clone();
								Either::Right(object)
							},
							Either::Right(id) => Either::Right(tg::Object::with_id(id)),
						};
						Ok::<_, tg::Error>((reference, tg::Referent { item, path, tag }))
					})
					.try_collect()?;
				let file = tg::graph::object::File {
					contents: tg::Blob::with_id(contents),
					dependencies,
					executable: file.executable,
				};
				tg::graph::Node::File(file)
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, subpath }) => {
				let artifact = match artifact {
					Either::Left(lockfile_index) if graph_indices.contains_key(lockfile_index) => {
						Either::Left(graph_indices.get(lockfile_index).copied().unwrap())
					},
					Either::Left(lockfile_index) => {
						let object = visited[*lockfile_index]
							.as_ref()
							.ok_or_else(|| tg::error!("expected an object"))?
							.clone()
							.try_into()?;
						Either::Right(object)
					},
					Either::Right(id) => {
						Either::Right(tg::Artifact::with_id(id.clone().try_into()?))
					},
				};
				let symlink = tg::graph::object::Symlink::Artifact {
					artifact,
					subpath: subpath.clone(),
				};
				tg::graph::Node::Symlink(symlink)
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { target }) => {
				let symlink = tg::graph::object::Symlink::Target {
					target: target.clone(),
				};
				tg::graph::Node::Symlink(symlink)
			},
		};
		nodes.push(node);
		Ok(())
	}

	pub fn create_normal_object_from_lockfile_node(
		lockfile: &tg::Lockfile,
		node: usize,
		visited: &mut [Option<tg::Object>],
	) -> tg::Result<tg::Object> {
		if let Some(visited) = visited[node].clone() {
			return Ok(visited);
		}
		let object: tg::Object = match &lockfile.nodes[node] {
			tg::lockfile::Node::Directory(directory) => {
				let entries = directory
					.entries
					.clone()
					.into_iter()
					.map(|(name, entry)| match entry {
						Either::Left(node) => {
							let object = visited[node]
								.clone()
								.ok_or_else(|| tg::error!("expected an object"))?;
							let artifact = tg::Artifact::try_from(object)?;
							Ok::<_, tg::Error>((name, artifact))
						},
						Either::Right(id) => {
							let id = id.try_into()?;
							let artifact = tg::Artifact::with_id(id);
							Ok::<_, tg::Error>((name, artifact))
						},
					})
					.try_collect()?;
				tg::Directory::with_entries(entries).into()
			},
			tg::lockfile::Node::File(file) => {
				let contents = file
					.contents
					.clone()
					.ok_or_else(|| tg::error!("expected a blob id"))?;
				let dependencies: BTreeMap<_, _> = file
					.dependencies
					.clone()
					.into_iter()
					.map(|(reference, referent)| {
						let tg::Referent { item, path, tag } = referent;
						let item = match item {
							Either::Left(node) => visited[node]
								.clone()
								.ok_or_else(|| tg::error!("expected an object"))?,
							Either::Right(id) => tg::Object::with_id(id),
						};
						Ok::<_, tg::Error>((reference, tg::Referent { item, path, tag }))
					})
					.try_collect()?;
				tg::File::builder(tg::Blob::with_id(contents))
					.dependencies(dependencies)
					.executable(file.executable)
					.build()
					.into()
			},
			tg::lockfile::Node::Symlink(symlink) => match symlink {
				tg::lockfile::Symlink::Artifact { artifact, subpath } => {
					let artifact = match artifact {
						Either::Left(node) => {
							let object = visited[*node]
								.clone()
								.ok_or_else(|| tg::error!("expected an object"))?;
							object.try_into()?
						},
						Either::Right(id) => tg::Artifact::with_id(id.clone().try_into()?),
					};
					tg::Symlink::with_artifact_and_subpath(artifact, subpath.clone()).into()
				},
				tg::lockfile::Symlink::Target { target } => {
					tg::Symlink::with_target(target.clone()).into()
				},
			},
		};
		visited[node].replace(object.clone());
		Ok(object)
	}
}

// Given a lockfile, get the paths of all the nodes.
fn get_paths(root_path: &Path, lockfile: &tg::Lockfile) -> tg::Result<Vec<Option<PathBuf>>> {
	fn get_paths_inner(
		lockfile: &tg::Lockfile,
		node_path: &Path,
		node: usize,
		visited: &mut Vec<Option<PathBuf>>,
	) -> tg::Result<()> {
		// Check if the node has been visited.
		if visited[node].is_some() {
			return Ok(());
		}

		// Check if the file system object exists. If it doesn't, leave the node empty.
		if !matches!(node_path.try_exists(), Ok(true)) {
			return Ok(());
		}

		// Update the visited set.
		visited[node].replace(node_path.to_owned());

		// Recurse.
		match &lockfile.nodes[node] {
			tg::lockfile::Node::Directory(tg::lockfile::Directory { entries, .. }) => {
				for (name, entry) in entries {
					let Either::Left(index) = entry else {
						continue;
					};
					let node_path = node_path.join(name);
					get_paths_inner(lockfile, &node_path, *index, visited)?;
				}
			},
			tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) => {
				for referent in dependencies.values() {
					let Either::Left(index) = &referent.item else {
						continue;
					};
					// Skip tags.
					if referent.tag.is_some() {
						continue;
					}
					let Some(path) = &referent.path else {
						continue;
					};
					let path = node_path.parent().unwrap().join(path);
					if !matches!(path.try_exists(), Ok(true)) {
						continue;
					}
					let Ok(node_path) = crate::util::fs::canonicalize_parent_sync(&path) else {
						continue;
					};
					get_paths_inner(lockfile, &node_path, *index, visited)?;
				}
			},
			tg::lockfile::Node::Symlink(_) => (),
		}

		Ok(())
	}

	if lockfile.nodes.is_empty() {
		return Ok(Vec::new());
	}

	let mut visited = vec![None; lockfile.nodes.len()];
	let node = 0;
	get_paths_inner(lockfile, root_path, node, &mut visited)?;
	Ok(visited)
}

#[derive(Copy, Clone)]
struct LockfileGraph<'a>(&'a [tg::lockfile::Node]);
impl petgraph::visit::GraphBase for LockfileGraph<'_> {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}
impl petgraph::visit::GraphRef for LockfileGraph<'_> {}
impl petgraph::visit::NodeIndexable for LockfileGraph<'_> {
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
impl petgraph::visit::IntoNeighbors for LockfileGraph<'_> {
	type Neighbors = std::vec::IntoIter<usize>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		match &self.0[a] {
			tg::lockfile::Node::Directory(directory) => directory
				.entries
				.values()
				.filter_map(|entry| entry.as_ref().left().copied())
				.collect::<Vec<_>>()
				.into_iter(),
			tg::lockfile::Node::File(file) => file
				.dependencies
				.values()
				.filter_map(|referent| referent.item.as_ref().left().copied())
				.collect::<Vec<_>>()
				.into_iter(),
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
				artifact: Either::Left(node),
				..
			}) => vec![*node].into_iter(),
			tg::lockfile::Node::Symlink(_) => Vec::new().into_iter(),
		}
	}
}
impl petgraph::visit::IntoNodeIdentifiers for LockfileGraph<'_> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.len()
	}
}
