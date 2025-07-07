use crate::Server;
use itertools::Itertools;
use std::{
	collections::{BTreeMap, HashMap},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_either::Either;

#[derive(Clone, Debug)]
pub(crate) struct Lockfile {
	pub objects: Vec<Option<tg::Object>>,
	pub nodes: Vec<tg::lockfile::Node>,
	pub paths: Vec<Option<PathBuf>>,
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
		let mut objects = Vec::new();
		let mut visited = HashMap::default();
		let mut graphs = HashMap::default();

		// Create nodes in the lockfile for the graph.
		let root = self.get_or_create_lockfile_node_for_artifact(
			artifact.clone(),
			true,
			checkout_dependencies,
			&mut nodes,
			&mut objects,
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
		let nodes = Self::strip_lockfile_nodes(&nodes, &objects, root)?;

		// Create the lockfile.
		let lockfile = tg::Lockfile { nodes };

		Ok(lockfile)
	}

	#[allow(clippy::too_many_arguments)]
	fn get_or_create_lockfile_node_for_artifact(
		&self,
		id: tg::artifact::Id,
		is_path_dependency: bool,
		checkout_dependencies: bool,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
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
					let tg::directory::Data::Graph(tg::graph::data::Ref { graph, node }) =
						directory
					else {
						break 'a;
					};
					(graph, *node)
				},
				tg::artifact::Data::File(file) => {
					let tg::file::Data::Graph(data) = file else {
						break 'a;
					};
					(&data.graph, data.node)
				},
				tg::artifact::Data::Symlink(symlink) => {
					let tg::symlink::Data::Graph(data) = symlink else {
						break 'a;
					};
					(&data.graph, data.node)
				},
			};
			let graph = graph.as_ref().unwrap();
			let nodes = self.create_lockfile_node_with_graph(
				graph,
				checkout_dependencies,
				is_path_dependency,
				nodes,
				objects,
				visited,
				graphs,
			)?;
			return Ok(nodes[node]);
		}

		// Only create a distinct node for non-graph artifacts.
		nodes.push(None);
		objects.push((!is_path_dependency).then(|| id.clone().into()));
		visited.insert(id, index);

		// Create a new lockfile node for the artifact, recursing over dependencies.
		let node = match &artifact {
			tg::artifact::Data::Directory(directory) => {
				let tg::directory::data::Directory::Node(tg::directory::data::Node { entries }) =
					directory
				else {
					unreachable!()
				};
				let mut entries_ = BTreeMap::new();
				for (name, edge) in entries {
					let index = match edge {
						tg::graph::data::Edge::Graph(edge) => {
							let Some(graph) = &edge.graph else { todo!() };
							graphs.get(graph).unwrap()[edge.node]
						},
						tg::graph::data::Edge::Object(edge) => self
							.get_or_create_lockfile_node_for_artifact(
								edge.clone(),
								is_path_dependency,
								checkout_dependencies,
								nodes,
								objects,
								visited,
								graphs,
							)?,
					};
					entries_.insert(name.clone(), Either::Left(index));
				}
				tg::lockfile::Node::Directory(tg::lockfile::Directory { entries: entries_ })
			},

			tg::artifact::Data::File(file) => {
				let tg::file::data::File::Node(data) = file else {
					unreachable!()
				};
				let contents = &data.contents;
				let dependencies = &data.dependencies;
				let executable = data.executable;
				let mut dependencies_ = BTreeMap::new();
				for (reference, referent) in dependencies {
					let is_path_dependency = is_path_dependency && reference.path().is_some();
					let id = match &referent.item {
						tg::graph::data::Edge::Graph(_edge) => {
							todo!()
						},
						tg::graph::data::Edge::Object(edge) => edge.clone(),
					};
					let item = match id {
						tg::object::Id::Directory(id) if checkout_dependencies => {
							let index = self.get_or_create_lockfile_node_for_artifact(
								id.clone().into(),
								is_path_dependency,
								checkout_dependencies,
								nodes,
								objects,
								visited,
								graphs,
							)?;
							Either::Left(index)
						},
						tg::object::Id::File(id) if checkout_dependencies => {
							let index = self.get_or_create_lockfile_node_for_artifact(
								id.clone().into(),
								is_path_dependency,
								checkout_dependencies,
								nodes,
								objects,
								visited,
								graphs,
							)?;
							Either::Left(index)
						},
						tg::object::Id::Symlink(id) if checkout_dependencies => {
							let index = self.get_or_create_lockfile_node_for_artifact(
								id.clone().into(),
								is_path_dependency,
								checkout_dependencies,
								nodes,
								objects,
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
				let contents = contents.clone();
				tg::lockfile::Node::File(tg::lockfile::File {
					contents,
					dependencies: dependencies_,
					executable,
				})
			},

			tg::artifact::Data::Symlink(symlink) => match symlink {
				tg::symlink::data::Symlink::Graph(..) => unreachable!(),
				tg::symlink::data::Symlink::Node(data) => {
					let artifact = &data.artifact;
					let path = &data.path;
					let artifact = if let Some(edge) = artifact {
						let artifact = match edge {
							tg::graph::data::Edge::Graph(_edge) => {
								todo!()
							},
							tg::graph::data::Edge::Object(edge) => edge.clone(),
						};
						let index = self.get_or_create_lockfile_node_for_artifact(
							artifact,
							false,
							checkout_dependencies,
							nodes,
							objects,
							visited,
							graphs,
						)?;
						Some(Either::Left(index))
					} else {
						None
					};
					let path = path.as_ref().map(PathBuf::from);
					tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
						artifact,
						path: path.clone(),
					})
				},
			},
		};

		// Update the visited set.
		nodes[index].replace(node);

		Ok(index)
	}

	#[allow(clippy::too_many_arguments)]
	fn create_lockfile_node_with_graph(
		&self,
		id: &tg::graph::Id,
		is_path_dependency: bool,
		checkout_dependencies: bool,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		objects: &mut Vec<Option<tg::object::Id>>,
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
				tg::artifact::Kind::Directory => tg::directory::Data::Graph(tg::graph::data::Ref {
					graph: Some(id.clone()),
					node,
				})
				.into(),
				tg::artifact::Kind::File => tg::file::Data::Graph(tg::graph::data::Ref {
					graph: Some(id.clone()),
					node,
				})
				.into(),
				tg::artifact::Kind::Symlink => tg::symlink::Data::Graph(tg::graph::data::Ref {
					graph: Some(id.clone()),
					node,
				})
				.into(),
			};
			let bytes = data.serialize()?;
			let id = tg::artifact::Id::new(kind, &bytes);

			let index = visited.get(&id).copied().unwrap_or_else(|| {
				let index = nodes.len();
				visited.insert(id.clone(), index);
				nodes.push(None);
				objects.push((!is_path_dependency).then(|| id.clone().into()));
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
					for (name, edge) in &directory.entries {
						let index = match edge {
							tg::graph::data::Edge::Graph(edge) => {
								if let Some(_graph) = &edge.graph {
									todo!()
								} else {
									indices[edge.node]
								}
							},
							tg::graph::data::Edge::Object(edge) => self
								.get_or_create_lockfile_node_for_artifact(
									edge.clone(),
									is_path_dependency,
									checkout_dependencies,
									nodes,
									objects,
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
							tg::graph::data::Edge::Graph(edge) => {
								if let Some(_graph) = &edge.graph {
									todo!()
								} else {
									Either::Left(indices[edge.node])
								}
							},
							tg::graph::data::Edge::Object(object) => match object {
								tg::object::Id::Directory(id) => {
									let index = self.get_or_create_lockfile_node_for_artifact(
										id.clone().into(),
										is_path_dependency,
										checkout_dependencies,
										nodes,
										objects,
										visited,
										graphs,
									)?;
									Either::Left(index)
								},
								tg::object::Id::File(id) => {
									let index = self.get_or_create_lockfile_node_for_artifact(
										id.clone().into(),
										is_path_dependency,
										checkout_dependencies,
										nodes,
										objects,
										visited,
										graphs,
									)?;
									Either::Left(index)
								},
								tg::object::Id::Symlink(id) => {
									let index = self.get_or_create_lockfile_node_for_artifact(
										id.clone().into(),
										is_path_dependency,
										checkout_dependencies,
										nodes,
										objects,
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
						contents,
						dependencies,
						executable,
					})
				},

				tg::graph::data::Node::Symlink(symlink) => {
					let artifact = if let Some(edge) = &symlink.artifact {
						let artifact = match &edge {
							tg::graph::data::Edge::Graph(edge) => {
								if let Some(_graph) = &edge.graph {
									todo!()
								} else {
									indices[edge.node]
								}
							},
							tg::graph::data::Edge::Object(artifact) => self
								.get_or_create_lockfile_node_for_artifact(
									artifact.clone(),
									is_path_dependency,
									checkout_dependencies,
									nodes,
									objects,
									visited,
									graphs,
								)?,
						};
						let artifact = Either::Left(artifact);
						Some(artifact)
					} else {
						None
					};
					let path = symlink.path.clone();
					tg::lockfile::Node::Symlink(tg::lockfile::Symlink { artifact, path })
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

			tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
				artifact,
				path: path_,
			}) => {
				// Get the referent artifact.
				let Some(Either::Left(artifact)) = artifact else {
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
				let subpath = path_.as_deref().unwrap_or("".as_ref());
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

impl Lockfile {
	pub fn get_node_for_path(&self, node_path: &Path) -> tg::Result<usize> {
		// Linear search, which should be faster than looking in a btreemap. Replace with a btreemap if this is too slow.
		self.paths
			.iter()
			.position(|path| path.as_deref() == Some(node_path))
			.ok_or_else(|| tg::error!(%path = node_path.display(), "failed to find in lockfile"))
	}
}

impl Server {
	pub(crate) fn try_parse_lockfile(&self, path: &Path) -> tg::Result<Option<Lockfile>> {
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
					let ids = entries
						.values()
						.filter_map(|value| value.as_ref().right().cloned());
					object_ids.extend(ids);
				},
				tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) => {
					let ids =
						dependencies
							.values()
							.filter_map(|referent| match referent.item.clone() {
								Either::Right(tg::object::Id::Directory(id)) => Some(id.into()),
								Either::Right(tg::object::Id::File(id)) => Some(id.into()),
								Either::Right(tg::object::Id::Symlink(id)) => Some(id.into()),
								_ => None,
							});
					object_ids.extend(ids);
				},
				tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
					artifact: Some(Either::Right(artifact)),
					..
				}) => {
					object_ids.push(artifact.clone());
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

		let objects = Self::create_objects_from_lockfile(&lockfile.nodes)
			.map_err(|source| tg::error!(!source, "failed to create objects from lockfile"))?;

		// Create the parsed lockfile.
		let lockfile = Lockfile {
			nodes: lockfile.nodes,
			objects,
			paths,
		};

		Ok(Some(lockfile))
	}
}

impl Server {
	pub(crate) fn create_object_from_lockfile_node(
		nodes: &[tg::lockfile::Node],
		node: usize,
	) -> tg::Result<tg::Object> {
		Self::create_objects_from_lockfile(nodes)?[node]
			.clone()
			.ok_or_else(|| tg::error!("failed to create the object"))
	}

	fn create_objects_from_lockfile(
		nodes: &[tg::lockfile::Node],
	) -> tg::Result<Vec<Option<tg::Object>>> {
		let mut visited = vec![None; nodes.len()];

		// Create graphs.
		let sccs = petgraph::algo::tarjan_scc(LockfileGraph(nodes));

		for mut scc in sccs {
			// Create normal objects for existing items.
			if scc.len() == 1 {
				Self::try_create_normal_object_from_lockfile_node(nodes, scc[0], &mut visited)?;
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
			let mut graph_nodes = Vec::with_capacity(scc.len());
			for lockfile_index in &scc {
				Self::try_create_graph_node_from_lockfile_node(
					nodes,
					*lockfile_index,
					&graph_indices,
					&mut graph_nodes,
					&mut visited,
				)?;
			}

			// Construct the graph.
			let graph = tg::Graph::with_nodes(graph_nodes);

			// Construct the objects.
			for (graph_index, lockfile_index) in scc.into_iter().enumerate() {
				let object = match &nodes[lockfile_index] {
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

		Ok(visited)
	}

	fn try_create_graph_node_from_lockfile_node(
		lockfile_nodes: &[tg::lockfile::Node],
		lockfile_index: usize,
		graph_indices: &BTreeMap<usize, usize>,
		graph_nodes: &mut Vec<tg::graph::Node>,
		visited: &mut [Option<tg::Object>],
	) -> tg::Result<()> {
		let node = match &lockfile_nodes[lockfile_index] {
			tg::lockfile::Node::Directory(directory) => {
				let Ok(entries) = directory
					.entries
					.clone()
					.into_iter()
					.map(|(name, entry)| {
						let entry = match entry {
							Either::Left(lockfile_index)
								if graph_indices.contains_key(&lockfile_index) =>
							{
								tg::graph::object::Edge::Graph(tg::graph::object::Ref {
									graph: None,
									node: graph_indices.get(&lockfile_index).copied().unwrap(),
								})
							},
							Either::Left(lockfile_index) => {
								let object = visited[lockfile_index]
									.as_ref()
									.ok_or_else(|| tg::error!("expected an object"))?
									.clone()
									.try_into()?;
								tg::graph::object::Edge::Object(object)
							},
							Either::Right(id) => {
								let artifact = tg::Artifact::with_id(id.clone().try_into()?);
								tg::graph::object::Edge::Object(artifact)
							},
						};
						Ok::<_, tg::Error>((name, entry))
					})
					.try_collect()
				else {
					return Ok(());
				};
				let directory = tg::graph::object::Directory { entries };
				tg::graph::Node::Directory(directory)
			},
			tg::lockfile::Node::File(file) => {
				let Some(contents) = file.contents.clone() else {
					return Ok(());
				};
				let Ok(dependencies) = file
					.dependencies
					.clone()
					.into_iter()
					.map(|(reference, referent)| {
						let tg::Referent { item, path, tag } = referent;
						let item = match item {
							Either::Left(lockfile_index)
								if graph_indices.contains_key(&lockfile_index) =>
							{
								tg::graph::object::Edge::Graph(tg::graph::object::Ref {
									graph: None,
									node: graph_indices.get(&lockfile_index).copied().unwrap(),
								})
							},
							Either::Left(lockfile_index) => {
								let object = visited[lockfile_index]
									.as_ref()
									.ok_or_else(|| tg::error!("expected an object"))?
									.clone();
								tg::graph::object::Edge::Object(object)
							},
							Either::Right(id) => {
								tg::graph::object::Edge::Object(tg::Object::with_id(id))
							},
						};
						Ok::<_, tg::Error>((reference, tg::Referent { item, path, tag }))
					})
					.try_collect()
				else {
					return Ok(());
				};
				let file = tg::graph::object::File {
					contents: tg::Blob::with_id(contents),
					dependencies,
					executable: file.executable,
				};
				tg::graph::Node::File(file)
			},
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink { artifact, path }) => {
				let artifact = match artifact {
					Some(Either::Left(lockfile_index))
						if graph_indices.contains_key(lockfile_index) =>
					{
						let artifact = tg::graph::object::Edge::Graph(tg::graph::object::Ref {
							graph: None,
							node: graph_indices.get(lockfile_index).copied().unwrap(),
						});
						Some(artifact)
					},
					Some(Either::Left(lockfile_index)) => {
						let object = visited[*lockfile_index]
							.as_ref()
							.ok_or_else(|| tg::error!("expected an object"))?
							.clone()
							.try_into()?;
						Some(tg::graph::object::Edge::Object(object))
					},
					Some(Either::Right(id)) => {
						let artifact = tg::Artifact::with_id(id.clone().try_into()?);
						Some(tg::graph::object::Edge::Object(artifact))
					},
					None => None,
				};
				let path = path.clone();
				let symlink = tg::graph::object::Symlink { artifact, path };
				tg::graph::Node::Symlink(symlink)
			},
		};
		graph_nodes.push(node);
		Ok(())
	}

	fn try_create_normal_object_from_lockfile_node(
		nodes: &[tg::lockfile::Node],
		node: usize,
		visited: &mut [Option<tg::Object>],
	) -> tg::Result<()> {
		let object: tg::Object = match &nodes[node] {
			tg::lockfile::Node::Directory(directory) => {
				let Ok(entries) = directory
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
					.try_collect()
				else {
					return Ok(());
				};
				tg::Directory::with_entries(entries).into()
			},
			tg::lockfile::Node::File(file) => {
				let Some(contents) = file.contents.clone() else {
					return Ok(());
				};
				let Ok(dependencies) = file
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
					.try_collect::<_, BTreeMap<_, _>, _>()
				else {
					return Ok(());
				};
				tg::File::builder(tg::Blob::with_id(contents))
					.dependencies(dependencies)
					.executable(file.executable)
					.build()
					.into()
			},
			tg::lockfile::Node::Symlink(symlink) => {
				let artifact = match &symlink.artifact {
					Some(Either::Left(node)) => {
						let object = visited[*node]
							.clone()
							.ok_or_else(|| tg::error!("expected an object"))?;
						let artifact = object.try_into()?;
						Some(tg::graph::object::Edge::Object(artifact))
					},
					Some(Either::Right(id)) => {
						let id = id.clone().try_into()?;
						let artifact = tg::Artifact::with_id(id);
						Some(tg::graph::object::Edge::Object(artifact))
					},
					None => None,
				};
				let path = symlink.path.clone();
				tg::Symlink::with_object(tg::symlink::Object::Node(tg::symlink::object::Node {
					artifact,
					path,
				}))
				.into()
			},
		};
		visited[node].replace(object);
		Ok(())
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
		// Avoid cycles.
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
		let neighbors = match &self.0[a] {
			tg::lockfile::Node::Directory(directory) => directory
				.entries
				.values()
				.filter_map(|entry| entry.as_ref().left().copied())
				.collect::<Vec<_>>(),
			tg::lockfile::Node::File(file) => file
				.dependencies
				.values()
				.filter_map(|referent| referent.item.as_ref().left().copied())
				.collect::<Vec<_>>(),
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink {
				artifact: Some(Either::Left(node)),
				..
			}) => vec![*node],
			tg::lockfile::Node::Symlink(_) => Vec::new(),
		};
		neighbors.into_iter()
	}
}
impl petgraph::visit::IntoNodeIdentifiers for LockfileGraph<'_> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.len()
	}
}

#[cfg(test)]
mod tests {
	use crate::test::test;
	use indoc::indoc;
	use insta::assert_snapshot;
	use tangram_client as tg;
	use tangram_temp as temp;

	#[tokio::test]
	async fn parse() {
		test(async move |context| {
			let server = context.start_server().await;
			let artifact: temp::Artifact = temp::directory! {
				"tangram.lock" => indoc!(r#"
					{
					  "nodes": [
					    {
					     "kind": "directory",
					     "entries": {
					      "tangram.ts": 1
					     }
					    },
					    {
					     "kind": "file",
					     "dependencies": {
					      "a/^1": {
					      	"item": 2,
					      	"path": "tangram.ts",
					      	"tag": "a/1.0.0"
					      }
					     }
					    },
					    {
					     "kind": "file",
					     "contents": "blb_01qc4k8f53qz0mh9e1wwmymcqj99bebd2r9t65g8ry4a2bx1hcr2v0"
					    }
					  ]
	  			}
				"#),
			}
			.into();
			let temp = temp::Temp::new();
			artifact.to_path(temp.path()).await.unwrap();
			let lockfile = server
				.try_parse_lockfile(temp.path())
				.expect("failed to parse lockfile")
				.expect("failed to parse lockfile");
			assert_eq!(lockfile.paths[0], Some(temp.path().to_owned()));
			let value = lockfile
				.objects
				.iter()
				.map(|object| match object {
					Some(object) => tg::Value::Object(object.clone()),
					None => tg::Value::Null,
				})
				.collect::<Vec<_>>();
			let output = tg::Value::Array(value).print(tg::value::print::Options {
				depth: Some(1),
				style: tg::value::print::Style::Pretty { indentation: "  " },
			});
			assert_snapshot!(&output, @r#"
			[
			  null,
			  null,
			  tg.file({
			    "contents": blb_01qc4k8f53qz0mh9e1wwmymcqj99bebd2r9t65g8ry4a2bx1hcr2v0,
			  }),
			]
			"#);
		})
		.await;
	}

	#[tokio::test]
	async fn parse_with_cycles() {
		test(async move |context| {
			let server = context.start_server().await;
			let artifact: temp::Artifact = temp::directory! {
				"left" => temp::file!(""),
				"right" => temp::file!(""),
				"tangram.lock" => temp::file!(indoc!(r#"
					{
						"nodes": [
							{
								"kind": "directory",
								"entries": {
									"left": 1,
									"right": 2
								}
							},
							{
								"kind": "file",
								"dependencies": {
									"./left": {
										"item": 2,
										"path": "right"
									}
								}
							},
							{
								"kind": "file",
								"dependencies": {
									"./right": {
										"item": 1,
										"path": "left"
									}
								}
							}
						]
					}
				"#))
			}
			.into();
			let temp = temp::Temp::new();
			artifact.to_path(temp.path()).await.unwrap();
			let lockfile = server
				.try_parse_lockfile(temp.path())
				.expect("failed to parse lockfile")
				.expect("failed to parse lockfile");
			assert_eq!(lockfile.paths[1], Some(temp.path().join("left")));
			assert_eq!(lockfile.paths[2], Some(temp.path().join("right")));
		})
		.await;
	}
}
