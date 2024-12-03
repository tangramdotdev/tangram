use crate::Server;
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;

/// Represents a lockfile that's been parsed, containing all of its objects and paths to artifacts that it references.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ParsedLockfile {
	pub artifacts: BTreeMap<tg::artifact::Id, PathBuf>,
	pub graphs: BTreeMap<tg::graph::Id, Arc<tg::graph::Data>>,
	pub nodes: Vec<tg::lockfile::Node>,
	pub objects: Vec<Option<(tg::object::Id, tg::object::Data)>>,
	pub paths: Vec<Option<PathBuf>>,
	pub path: PathBuf,
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

pub(crate) struct LockfileNode {
	pub node: usize,
	pub package: PathBuf,
	pub path: PathBuf,
}

impl Server {
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

	pub(crate) async fn find_path_in_lockfile(
		&self,
		node: usize,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<PathBuf> {
		let result = self
			.find_node_in_lockfile(Either::Left(node), lockfile_path, lockfile)
			.await?;
		Ok(result.path)
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
					package: arg.current_package_path,
					path: arg.current_node_path,
				};
				return Ok(Some(result));
			},
			Either::Right(path) if path == arg.current_node_path => {
				let result = LockfileNode {
					node: arg.current_node,
					package: arg.current_package_path,
					path: arg.current_node_path,
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
			tg::lockfile::Node::Directory { entries } => {
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

			tg::lockfile::Node::File { dependencies, .. } => {
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
					let path = reference
						.options()
						.and_then(|options| options.subpath.as_ref())
						.map_or_else(|| path.to_owned(), |subpath| path.join(subpath));
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

			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, subpath }) => {
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

impl ParsedLockfile {
	pub fn try_resolve_dependency(
		&self,
		node_path: &Path,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::Referent<tg::object::Id>>> {
		let node = self.get_node_for_path(node_path)?;

		// Dependency resolution is only valid for files.
		let tg::lockfile::Node::File { dependencies, .. } = &self.nodes[node] else {
			return Err(tg::error!(%path = node_path.display(), "expected a file node"))?;
		};

		// Lookup the dependency.
		let Some(referent) = dependencies.get(reference) else {
			return Ok(None);
		};

		// Resolve the item.
		let item = match &referent.item {
			Either::Left(index) => self.objects[*index]
				.as_ref()
				.ok_or_else(|| tg::error!("invalid lockfile"))?
				.0
				.clone(),
			Either::Right(object) => object.clone(),
		};

		// Construct the referent.
		let referent = tg::Referent {
			item,
			tag: referent.tag.clone(),
			path: referent.path.clone(),
			subpath: referent.subpath.clone(),
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
		self
			.paths
			.iter()
			.position(|path| path.as_deref() == Some(node_path))
			.ok_or_else(|| tg::error!(%path = node_path.display(), %lockfile = self.path.display(), "failed to find in lockfile"))
	}

	pub fn get_file_dependencies(
		&self,
		path: &Path,
	) -> tg::Result<Vec<(tg::Reference, tg::Referent<PathBuf>)>> {
		// If we can find this file by path in the lockfile, try and use its dependencies.
		if let Ok(node) = self.get_node_for_path(path) {
			let tg::lockfile::Node::File { dependencies, .. } = &self.nodes[node] else {
				return Err(tg::error!(%path = path.display(), "expected a file"));
			};
			let mut dependencies_ = Vec::with_capacity(dependencies.len());
			for (reference, dependency) in dependencies {
				let path = match &dependency.item {
					Either::Left(node) => self.get_path_for_node(*node)?,
					Either::Right(tg::object::Id::Directory(id)) => self
						.artifacts
						.get(&id.clone().into())
						.ok_or_else(|| tg::error!("missing artifact"))?
						.clone(),
					Either::Right(tg::object::Id::File(id)) => self
						.artifacts
						.get(&id.clone().into())
						.ok_or_else(|| tg::error!("missing artifact"))?
						.clone(),
					Either::Right(tg::object::Id::Symlink(id)) => self
						.artifacts
						.get(&id.clone().into())
						.ok_or_else(|| tg::error!("missing artifact"))?
						.clone(),
					Either::Right(_) => continue,
				};
				let dependency = tg::Referent {
					item: path,
					path: dependency.path.clone(),
					subpath: dependency.subpath.clone(),
					tag: dependency.tag.clone(),
				};
				dependencies_.push((reference.clone(), dependency));
			}

			return Ok(dependencies_);
		}

		if let Some(artifact) = self
			.artifacts
			.iter()
			.find_map(|(id, path_)| (path_ == path).then_some(id))
		{
			// Get the file data from the lockfile.
			let id: tg::object::Id = artifact.clone().into();
			let tg::object::Data::File(file) = self
				.objects
				.iter()
				.find_map(|object| {
					let (id_, data) = object.as_ref()?;
					(id_ == &id).then_some(data)
				})
				.ok_or_else(|| tg::error!("expected a file"))?
			else {
				return Err(tg::error!("could not find in lockfile"));
			};

			// Get the dependencies of this file as artifact IDs.
			let dependencies: Vec<(tg::Reference, tg::Referent<tg::artifact::Id>)> = match file {
				tg::file::Data::Graph { graph, node } => {
					let graph_data = self
						.graphs
						.get(graph)
						.ok_or_else(|| tg::error!("could not find graph in lockfile"))?;
					let node = graph_data
						.nodes
						.get(*node)
						.ok_or_else(|| tg::error!("could not find node in graph"))?;
					let tg::graph::data::Node::File(tg::graph::data::File { dependencies, .. }) =
						node
					else {
						return Err(tg::error!("expected a file"));
					};
					dependencies
						.iter()
						.filter_map(|(reference, referent)| {
							let id = match &referent.item {
								Either::Left(node) => match &graph_data.nodes[*node].kind() {
									tg::artifact::Kind::Directory => {
										let data = tg::directory::Data::Graph {
											graph: graph.clone(),
											node: *node,
										};
										let id = tg::directory::Id::new(&data.serialize().unwrap());
										id.into()
									},
									tg::artifact::Kind::File => {
										let data = tg::file::Data::Graph {
											graph: graph.clone(),
											node: *node,
										};
										let id = tg::file::Id::new(&data.serialize().unwrap());
										id.into()
									},
									tg::artifact::Kind::Symlink => {
										let data = tg::symlink::Data::Graph {
											graph: graph.clone(),
											node: *node,
										};
										let id = tg::symlink::Id::new(&data.serialize().unwrap());
										id.into()
									},
								},
								Either::Right(tg::object::Id::Directory(id)) => id.clone().into(),
								Either::Right(tg::object::Id::File(id)) => id.clone().into(),
								Either::Right(tg::object::Id::Symlink(id)) => id.clone().into(),
								Either::Right(_) => return None,
							};
							let dependency = tg::Referent {
								item: id,
								path: referent.path.clone(),
								subpath: referent.subpath.clone(),
								tag: referent.tag.clone(),
							};
							Some((reference.clone(), dependency))
						})
						.collect()
				},
				tg::file::Data::Normal { dependencies, .. } => dependencies
					.iter()
					.filter_map(|(reference, referent)| {
						let id = match &referent.item {
							tg::object::Id::Directory(id) => id.clone().into(),
							tg::object::Id::File(id) => id.clone().into(),
							tg::object::Id::Symlink(id) => id.clone().into(),
							_ => return None,
						};
						let dependency = tg::Referent {
							item: id,
							path: referent.path.clone(),
							subpath: referent.subpath.clone(),
							tag: referent.tag.clone(),
						};
						Some((reference.clone(), dependency))
					})
					.collect(),
			};
			return dependencies
				.into_iter()
				.map(|(reference, dependency)| {
					let path = self
						.artifacts
						.get(&dependency.item)
						.ok_or_else(|| tg::error!("missing artifact"))?
						.clone();
					let dependency = tg::Referent {
						item: path,
						path: dependency.path,
						subpath: dependency.subpath,
						tag: dependency.tag,
					};
					Ok::<_, tg::Error>((reference, dependency))
				})
				.try_collect();
		}

		Err(tg::error!("failed to get dependencies"))
	}
}

impl Server {
	pub async fn parse_lockfile(&self, path: &Path) -> tg::Result<ParsedLockfile> {
		// Read the lockfile from disk.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let contents = tokio::fs::read(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to read lockfile"),
		)?;
		drop(permit);

		// Deserialize the lockfile.
		let lockfile = serde_json::from_slice::<tg::Lockfile>(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize lockfile"),
		)?;

		// Extract the objects contained within the lockfile.
		let Objects { graphs, objects } = get_objects(&lockfile).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to parse lockfile"),
		)?;

		// Collect any IDs not contained within the lockfile.
		let graphs = graphs
			.into_iter()
			.map(|data| {
				let id = tg::graph::Id::new(&data.serialize()?);
				Ok::<_, tg::Error>((id, Arc::new(data)))
			})
			.try_collect()?;
		let objects: Vec<_> = objects
			.into_iter()
			.map(|data| {
				let Some(data) = data else { return Ok(None) };
				let id = tg::object::Id::new(data.kind(), &data.serialize()?);
				Ok::<_, tg::Error>(Some((id, data)))
			})
			.try_collect()?;

		// Get any referenced objects that are not contained within the lockfile.
		let mut referenced_objects = Vec::new();
		for node in &lockfile.nodes {
			match node {
				tg::lockfile::Node::Directory { entries } => {
					let it = entries
						.values()
						.filter_map(|value| value.as_ref().right().cloned());
					referenced_objects.extend(it);
				},
				tg::lockfile::Node::File { dependencies, .. } => {
					let it =
						dependencies
							.values()
							.filter_map(|referent| match referent.item.clone() {
								Either::Right(tg::object::Id::Directory(id)) => Some(id.into()),
								Either::Right(tg::object::Id::File(id)) => Some(id.into()),
								Either::Right(tg::object::Id::Symlink(id)) => Some(id.into()),
								_ => None,
							});
					referenced_objects.extend(it);
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
					artifact, ..
				}) => {
					let it = artifact.as_ref().right().cloned();
					referenced_objects.extend(it);
				},
				tg::lockfile::Node::Symlink(_) => continue,
			}
		}

		// Collect all the artifact IDs, implicit or explicit.
		let artifact_ids = objects
			.iter()
			.filter_map(|kv| Some(&kv.as_ref()?.0))
			.cloned()
			.chain(referenced_objects)
			.filter_map(|id| id.try_into().ok())
			.collect::<BTreeSet<tg::artifact::Id>>();

		// Get the artifacts path.
		let mut artifacts_path = None;
		for path in path.ancestors().skip(1) {
			let path = path.join(".tangram/artifacts");
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
				artifacts_path.replace(path);
				break;
			}
		}
		let artifacts_path = artifacts_path.unwrap_or_else(|| self.artifacts_path());

		// Find paths for any artifacts.
		let mut artifacts = BTreeMap::new();
		for id in artifact_ids {
			// Check if the file exists.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let path = artifacts_path.join(id.to_string());
			if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
				artifacts.insert(id, path);
			}
		}

		// Get the paths for the lockfile nodes.
		let paths = get_paths(&artifacts_path, path, &lockfile, &objects).await?;

		// Create the parsed lockfile.
		let lockfile = ParsedLockfile {
			artifacts,
			graphs,
			nodes: lockfile.nodes,
			objects,
			paths,
			path: path.to_owned(),
		};

		Ok(lockfile)
	}
}

// Given a lockfile, get the paths of all the nodes.
async fn get_paths(
	artifacts_path: &Path,
	lockfile_path: &Path,
	lockfile: &tg::Lockfile,
	objects: &[Option<(tg::object::Id, tg::object::Data)>],
) -> tg::Result<Vec<Option<PathBuf>>> {
	async fn get_paths_inner(
		artifacts_path: &Path,
		lockfile: &tg::Lockfile,
		node_path: &Path,
		node: usize,
		objects: &[Option<(tg::object::Id, tg::object::Data)>],
		visited: &mut Vec<Option<PathBuf>>,
	) -> tg::Result<()> {
		// Check if the node has been visited.
		if visited[node].is_some() {
			return Ok(());
		}

		// Check if the file system object exists.
		if !matches!(tokio::fs::try_exists(node_path).await, Ok(true)) {
			return Err(
				tg::error!(%path = node_path.display(), "expected a file system object at the path"),
			)?;
		};

		// Update the visited set.
		visited[node].replace(node_path.to_owned());

		// Recurse.
		match &lockfile.nodes[node] {
			tg::lockfile::Node::Directory { entries } => {
				for (name, entry) in entries {
					let Either::Left(index) = entry else {
						continue;
					};
					let node_path = node_path.join(name);
					Box::pin(get_paths_inner(
						artifacts_path,
						lockfile,
						&node_path,
						*index,
						objects,
						visited,
					))
					.await?;
				}
			},
			tg::lockfile::Node::File { dependencies, .. } => {
				'outer: for (reference, referent) in dependencies {
					let Either::Left(index) = &referent.item else {
						continue;
					};

					// Try and follow a relative path.
					'a: {
						let Some(path) = reference
							.item()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.options()?.path.as_ref())
						else {
							break 'a;
						};
						let path = node_path.parent().unwrap().join(path);
						let Some(node_path) =
							crate::util::fs::canonicalize_parent(&path).await.ok()
						else {
							break 'a;
						};
						if Box::pin(get_paths_inner(
							artifacts_path,
							lockfile,
							&node_path,
							*index,
							objects,
							visited,
						))
						.await
						.is_ok()
						{
							continue 'outer;
						}
					};

					// Try and get the path to the object.
					let object = &objects[*index]
						.as_ref()
						.ok_or_else(|| tg::error!("expected an object ID"))?
						.0;
					let node_path = artifacts_path.join(object.to_string());
					Box::pin(get_paths_inner(
						artifacts_path,
						lockfile,
						&node_path,
						*index,
						objects,
						visited,
					))
					.await?;
				}
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact {
				artifact: Either::Left(index),
				..
			}) => {
				// Try and get the path to the object.
				let object = &objects[*index]
					.as_ref()
					.ok_or_else(|| tg::error!("expected an object ID"))?
					.0;
				let node_path = artifacts_path.join(object.to_string());
				Box::pin(get_paths_inner(
					artifacts_path,
					lockfile,
					&node_path,
					*index,
					objects,
					visited,
				))
				.await?;
			},
			tg::lockfile::Node::Symlink(_) => (),
		};

		Ok(())
	}

	if lockfile.nodes.is_empty() {
		return Ok(Vec::new());
	}

	let mut visited = vec![None; lockfile.nodes.len()];
	let node_path = lockfile_path.parent().unwrap();
	let node = 0;
	Box::pin(get_paths_inner(
		artifacts_path,
		lockfile,
		node_path,
		node,
		objects,
		&mut visited,
	))
	.await?;
	Ok(visited)
}

/// The objects referenced by a lockfile.
struct Objects {
	/// The graphs contained within the lockfile.
	pub graphs: Vec<tg::graph::Data>,

	/// The object data for each node in the graph.
	pub objects: Vec<Option<tg::object::Data>>,
}

/// Given a lockfile, get or create the object data it refers to.
fn get_objects(lockfile: &tg::Lockfile) -> tg::Result<Objects> {
	// Internal. Create normal data for a lockfile node.
	fn create_normal_data(
		index: usize,
		node: &tg::lockfile::Node,
	) -> tg::Result<Option<tg::object::Data>> {
		match node {
			tg::lockfile::Node::Directory { entries } => {
				let entries = entries
					.iter()
					.map(|(name, entry)| {
						let entry = entry
							.as_ref()
							.right()
							.ok_or_else(|| tg::error!(%node = index, "expected an artifact ID"))?;
						let entry: tg::artifact::Id = entry.clone().try_into().map_err(
							|_| tg::error!(%node = index, %name, "expected an artifact ID"),
						)?;
						Ok::<_, tg::Error>((name.clone(), entry))
					})
					.try_collect()?;
				let data = tg::directory::Data::Normal { entries };
				Ok(Some(tg::object::Data::Directory(data)))
			},
			tg::lockfile::Node::File {
				contents,
				dependencies,
				executable,
			} => {
				// Ignore files missing their contents.
				let Some(contents) = contents.clone() else {
					return Ok(None);
				};

				let dependencies = dependencies
					.iter()
					.map(|(reference, referent)| {
						let item =
							referent.item.as_ref().right().ok_or_else(
								|| tg::error!(%node = index, "expected an object ID"),
							)?;
						let reference = reference.clone();
						let referent = tg::Referent {
							item: item.clone(),
							path: referent.path.clone(),
							subpath: referent.subpath.clone(),
							tag: referent.tag.clone(),
						};
						Ok::<_, tg::Error>((reference, referent))
					})
					.try_collect()?;
				let data = tg::file::Data::Normal {
					contents,
					dependencies,
					executable: *executable,
				};
				Ok(Some(tg::object::Data::File(data)))
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, subpath }) => {
				let artifact = artifact
					.clone()
					.right()
					.ok_or_else(|| tg::error!("expected an artifact ID"))?
					.try_into()?;
				let subpath = subpath.clone();
				let data = tg::symlink::Data::Artifact { artifact, subpath };
				Ok(Some(tg::object::Data::Symlink(data)))
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { target }) => {
				let data = tg::symlink::Data::Target {
					target: target.clone(),
				};
				Ok(Some(tg::object::Data::Symlink(data)))
			},
		}
	}

	// Internal. Create graph data for a lockfile node.
	fn create_graph_data(
		index: usize,
		node: &tg::lockfile::Node,
		graph_indices: &BTreeMap<usize, usize>,
		objects: &[Option<tg::object::Data>],
	) -> tg::Result<Option<tg::graph::data::Node>> {
		match node {
			tg::lockfile::Node::Directory { entries } => {
				let entries = entries
					.iter()
					.map(|(name, entry)| {
						let entry = match entry {
							Either::Left(index) => {
								get_lockfile_item(*index, graph_indices, objects)?
							},
							Either::Right(id) => Either::Right(id.clone().try_into()?),
						};
						let name = name.clone();
						Ok::<_, tg::Error>((name, entry))
					})
					.try_collect()
					.map_err(
						|source| tg::error!(!source, %node = index, "invalid directory entries"),
					)?;
				let data = tg::graph::data::Node::Directory(tg::graph::data::Directory { entries });
				Ok(Some(data))
			},
			tg::lockfile::Node::File {
				contents,
				dependencies,
				executable,
			} => {
				let Some(contents) = contents.clone() else {
					return Ok(None);
				};
				let dependencies = dependencies
					.iter()
					.map(|(reference, referent)| {
						let item = match &referent.item {
							Either::Left(index) => {
								get_lockfile_item(*index, graph_indices, objects)?
									.map_right(tg::object::Id::from)
							},
							Either::Right(id) => Either::Right(id.clone()),
						};
						let reference = reference.clone();
						let referent = tg::Referent {
							item,
							path: referent.path.clone(),
							subpath: referent.subpath.clone(),
							tag: referent.tag.clone(),
						};
						Ok::<_, tg::Error>((reference, referent))
					})
					.try_collect()
					.map_err(
						|source| tg::error!(!source, %node = index, "invalid file dependencies"),
					)?;
				let data = tg::graph::data::Node::File(tg::graph::data::File {
					contents,
					dependencies,
					executable: *executable,
				});
				Ok(Some(data))
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, subpath }) => {
				let artifact = match artifact {
					Either::Left(index) => get_lockfile_item(*index, graph_indices, objects)?,
					Either::Right(id) => Either::Right(id.clone().try_into()?),
				};
				let subpath = subpath.clone();
				let data = tg::graph::data::Node::Symlink(tg::graph::data::Symlink::Artifact {
					artifact,
					subpath,
				});
				Ok(Some(data))
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Target { target }) => {
				let data = tg::graph::data::Node::Symlink(tg::graph::data::Symlink::Target {
					target: target.clone(),
				});
				Ok(Some(data))
			},
		}
	}

	// Internal. Convert a lockfile index into either an index or object ID.
	fn get_lockfile_item(
		lockfile_index: usize,
		graph_indices: &BTreeMap<usize, usize>,
		objects: &[Option<tg::object::Data>],
	) -> tg::Result<Either<usize, tg::artifact::Id>> {
		if let Some(index) = graph_indices.get(&lockfile_index) {
			return Ok(Either::Left(*index));
		}

		if let Some(data) = &objects[lockfile_index] {
			let id = tg::object::Id::new(data.kind(), &data.serialize()?).try_into()?;
			return Ok(Either::Right(id));
		}

		Err(tg::error!("invalid lockfile"))
	}

	let mut graphs = Vec::new();
	let mut objects = vec![None; lockfile.nodes.len()];

	'outer: for mut scc in petgraph::algo::tarjan_scc(&LockfileGraphImpl(lockfile)) {
		scc.sort_unstable();

		// Skip SCCs that are missing file contents.
		if scc.len() == 1 {
			let data = create_normal_data(scc[0], &lockfile.nodes[scc[0]])
				.map_err(|source| tg::error!(!source, "invalid lockfile"))?;
			objects[scc[0]] = data;
			continue;
		}

		// Allocate space for the new nodes.
		let mut nodes = Vec::with_capacity(scc.len());

		// Assign indices.
		let indices = scc
			.iter()
			.copied()
			.enumerate()
			.map(|(graph_index, lockfile_index)| (lockfile_index, graph_index))
			.collect::<BTreeMap<_, _>>();

		// Create new nodes.
		for lockfile_index in scc.iter().copied() {
			let Some(node) = create_graph_data(
				lockfile_index,
				&lockfile.nodes[lockfile_index],
				&indices,
				&objects,
			)?
			else {
				continue 'outer;
			};
			nodes.push(node);
		}

		// Create the graph data.
		let data = tg::graph::Data { nodes };
		let id = tg::graph::Id::new(&data.serialize()?);

		// Create the objects' data.
		for (graph_index, lockfile_index) in scc.iter().copied().enumerate() {
			let data = match data.nodes[graph_index].kind() {
				tg::artifact::Kind::Directory => tg::directory::Data::Graph {
					graph: id.clone(),
					node: graph_index,
				}
				.into(),
				tg::artifact::Kind::File => tg::file::Data::Graph {
					graph: id.clone(),
					node: graph_index,
				}
				.into(),
				tg::artifact::Kind::Symlink => tg::symlink::Data::Graph {
					graph: id.clone(),
					node: graph_index,
				}
				.into(),
			};
			objects[lockfile_index].replace(data);
		}

		graphs.push(data);
	}

	// Collect all the objects.
	let objects = Objects { graphs, objects };
	Ok(objects)
}

struct LockfileGraphImpl<'a>(&'a tg::Lockfile);

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
		self.0.nodes.len()
	}

	fn to_index(self: &Self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a LockfileGraphImpl<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		match &self.0.nodes[a] {
			tg::lockfile::Node::Directory { entries } => {
				let it = entries
					.values()
					.filter_map(|entry| entry.as_ref().left().copied());
				Box::new(it)
			},
			tg::lockfile::Node::File { dependencies, .. } => {
				let it = dependencies
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
		0..self.0.nodes.len()
	}
}
