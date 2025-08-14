use crate::Server;
use fnv::FnvBuildHasher;
use itertools::Itertools;
use std::{
	collections::{BTreeMap, HashMap},
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug)]
pub(crate) struct Lock {
	pub objects: Vec<Option<tg::Object>>,
	pub nodes: Vec<tg::graph::data::Node>,
	pub paths: Vec<Option<PathBuf>>,
}

struct State {
	dependencies: bool,
	graphs: HashMap<tg::graph::Id, tg::graph::Data, FnvBuildHasher>,
	nodes: Vec<Option<tg::graph::data::Node>>,
	objects: Vec<Option<tg::object::Id>>,
	visited: HashMap<tg::artifact::Id, usize, FnvBuildHasher>,
}

#[derive(Clone)]
struct FindArg<'a> {
	current_node_path: PathBuf,
	current_node: usize,
	current_package_node: usize,
	current_package_path: PathBuf,
	nodes: &'a [tg::graph::data::Node],
	search: Either<usize, &'a Path>,
}

impl Server {
	pub(crate) fn create_lock(
		&self,
		artifact: &tg::artifact::Id,
		dependencies: bool,
	) -> tg::Result<tg::graph::Data> {
		// Create the state.
		let mut state = State {
			dependencies,
			graphs: HashMap::default(),
			nodes: Vec::new(),
			objects: Vec::new(),
			visited: HashMap::default(),
		};

		// Create nodes in the lock for the graph.
		let edge = tg::graph::data::Edge::Object(artifact.clone());
		let root = self.create_lock_inner(&mut state, &edge, true)?;
		let nodes: Vec<_> = state
			.nodes
			.into_iter()
			.enumerate()
			.map(|(index, node)| {
				node.ok_or_else(
					|| tg::error!(%node = index, "invalid graph, failed to create lock node"),
				)
			})
			.collect::<tg::Result<_>>()?;

		// Strip.
		let nodes = strip(&nodes, &state.objects, root);

		// Create the lock.
		let lock = tg::graph::Data { nodes };

		Ok(lock)
	}

	fn create_lock_inner(
		&self,
		state: &mut State,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		is_path_dependency: bool,
	) -> tg::Result<usize> {
		let (id, node, graph) = match edge {
			tg::graph::data::Edge::Reference(reference) => {
				// Compute the ID.
				let graph = reference
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?;
				self.create_lock_ensure_graph_exists(state, graph)?;

				// Get the node data.
				let node = state
					.graphs
					.get(graph)
					.unwrap()
					.nodes
					.get(reference.node)
					.ok_or_else(|| tg::error!("invalid node index"))?;

				// Compute the id.
				let data: tg::artifact::data::Artifact = match node.kind() {
					tg::artifact::Kind::Directory => {
						tg::directory::Data::Reference(reference.clone()).into()
					},
					tg::artifact::Kind::File => tg::file::Data::Reference(reference.clone()).into(),
					tg::artifact::Kind::Symlink => {
						tg::symlink::Data::Reference(reference.clone()).into()
					},
				};
				let id = tg::artifact::Id::new(node.kind(), &data.serialize()?);

				// Check if visited.
				if let Some(index) = state.visited.get(&id).copied() {
					return Ok(index);
				}

				(id, node.clone(), Some(graph.clone()))
			},
			tg::graph::data::Edge::Object(id) => {
				// Check if visited.
				if let Some(index) = state.visited.get(id).copied() {
					return Ok(index);
				}

				// Otherwise, lookup the artifact data by ID.
				#[allow(clippy::match_wildcard_for_single_variants)]
				let data = match &self.store {
					crate::Store::Lmdb(store) => {
						store.try_get_object_data_sync(&id.clone().into())?
					},
					crate::Store::Memory(store) => store.try_get_object_data(&id.clone().into())?,
					_ => {
						return Err(tg::error!("unimplemented"));
					},
				}
				.ok_or_else(|| tg::error!(%id = id.clone(), "expected the object to be stored"))?;
				let data = tg::artifact::Data::try_from(data)?;

				match data {
					// Handle the case where this points into a graph.
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Reference(
						reference,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Reference(reference))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Reference(
						reference,
					)) => {
						let edge = tg::graph::data::Edge::Reference(reference);
						return self.create_lock_inner(state, &edge, is_path_dependency);
					},
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						(id.clone(), tg::graph::data::Node::Directory(node), None)
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						(id.clone(), tg::graph::data::Node::File(node), None)
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						(id.clone(), tg::graph::data::Node::Symlink(node), None)
					},
				}
			},
		};

		let index = state.nodes.len();
		state.visited.insert(id.clone(), index);
		state.nodes.push(None);
		state
			.objects
			.push((!is_path_dependency).then(|| id.clone().into()));
		let lock_node = match node {
			tg::graph::data::Node::Directory(node) => {
				self.create_lock_directory(state, &node, graph.as_ref(), is_path_dependency)?
			},
			tg::graph::data::Node::File(node) => {
				self.create_lock_file(state, &node, graph.as_ref(), is_path_dependency)?
			},
			tg::graph::data::Node::Symlink(node) => {
				self.create_lock_symlink(state, &node, graph.as_ref(), is_path_dependency)?
			},
		};
		state.nodes[index].replace(lock_node);
		Ok(index)
	}

	fn create_lock_ensure_graph_exists(
		&self,
		state: &mut State,
		graph: &tg::graph::Id,
	) -> tg::Result<()> {
		if state.graphs.contains_key(graph) {
			return Ok(());
		}
		#[allow(clippy::match_wildcard_for_single_variants)]
		let data = match &self.store {
			crate::Store::Lmdb(store) => store.try_get_object_data_sync(&graph.clone().into())?,
			crate::Store::Memory(store) => store.try_get_object_data(&graph.clone().into())?,
			_ => {
				return Err(tg::error!("unimplemented"));
			},
		}
		.ok_or_else(|| tg::error!("expected the object to be stored"))?
		.try_into()
		.map_err(|_| tg::error!("expected a graph"))?;
		state.graphs.insert(graph.clone(), data);
		Ok(())
	}

	fn create_lock_directory(
		&self,
		state: &mut State,
		node: &tg::graph::data::Directory,
		graph: Option<&tg::graph::Id>,
		is_path_dependency: bool,
	) -> tg::Result<tg::graph::data::Node> {
		let entries = node
			.entries
			.iter()
			.map(|(name, edge)| {
				let mut edge = edge.clone();
				if let tg::graph::data::Edge::Reference(reference) = &mut edge {
					if reference.graph.is_none() {
						reference.graph = graph.cloned();
					}
				}
				let node = self.create_lock_inner(state, &edge, is_path_dependency)?;
				let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: None,
					node,
				});
				Ok::<_, tg::Error>((name.clone(), edge))
			})
			.collect::<tg::Result<_>>()?;
		Ok(tg::graph::data::Node::Directory(
			tg::graph::data::Directory { entries },
		))
	}

	fn create_lock_file(
		&self,
		state: &mut State,
		node: &tg::graph::data::File,
		graph: Option<&tg::graph::Id>,
		is_path_dependency: bool,
	) -> tg::Result<tg::graph::data::Node> {
		let contents = node.contents.clone();
		let dependencies = node
			.dependencies
			.iter()
			.map(|(reference, referent)| {
				let edge = match referent.item.clone() {
					tg::graph::data::Edge::Reference(mut reference) => {
						if reference.graph.is_none() {
							reference.graph = graph.cloned();
						}
						tg::graph::data::Edge::Reference(reference)
					},
					tg::graph::data::Edge::Object(tg::object::Id::Directory(id))
						if state.dependencies =>
					{
						tg::graph::data::Edge::Object(id.into())
					},
					tg::graph::data::Edge::Object(tg::object::Id::File(id))
						if state.dependencies =>
					{
						tg::graph::data::Edge::Object(id.into())
					},
					tg::graph::data::Edge::Object(tg::object::Id::Symlink(id))
						if state.dependencies =>
					{
						tg::graph::data::Edge::Object(id.into())
					},
					tg::graph::data::Edge::Object(id) => {
						let referent = referent.clone().map(|_| tg::graph::data::Edge::Object(id));
						return Ok((reference.clone(), referent));
					},
				};
				let is_path_dependency = is_path_dependency
					&& (reference.options().local.is_some() || reference.item().is_path());
				let node = self.create_lock_inner(state, &edge, is_path_dependency)?;
				let referent = referent.clone().map(|_| {
					tg::graph::data::Edge::Reference(tg::graph::data::Reference {
						graph: None,
						node,
					})
				});
				Ok::<_, tg::Error>((reference.clone(), referent))
			})
			.collect::<tg::Result<_>>()?;
		let executable = node.executable;
		Ok(tg::graph::data::Node::File(tg::graph::data::File {
			contents,
			dependencies,
			executable,
		}))
	}

	fn create_lock_symlink(
		&self,
		state: &mut State,
		node: &tg::graph::data::Symlink,
		graph: Option<&tg::graph::Id>,
		is_path_dependency: bool,
	) -> tg::Result<tg::graph::data::Node> {
		let artifact = node
			.artifact
			.as_ref()
			.map(|edge| {
				let mut edge = edge.clone();
				if let tg::graph::data::Edge::Reference(reference) = &mut edge {
					if reference.graph.is_none() {
						reference.graph = graph.cloned();
					}
				}
				self.create_lock_inner(state, &edge, is_path_dependency)
			})
			.transpose()?
			.map(|node| {
				tg::graph::data::Edge::Reference(tg::graph::data::Reference { graph: None, node })
			});
		Ok(tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
			artifact,
			path: node.path.clone(),
		}))
	}

	pub(crate) async fn find_node_in_lock(
		&self,
		search: Either<usize, &Path>,
		lock_path: &Path,
		lock: &tg::graph::Data,
	) -> tg::Result<usize> {
		let current_package_path = lock_path.parent().unwrap().to_owned();
		let current_package_node = 0;
		if lock.nodes.is_empty() {
			return Err(tg::error!("invalid lock"));
		}
		let mut visited = vec![false; lock.nodes.len()];
		let arg = FindArg {
			current_node_path: current_package_path.clone(),
			current_node: current_package_node,
			current_package_path,
			current_package_node,
			nodes: &lock.nodes,
			search,
		};
		self.find_node_in_lock_inner(arg.clone(), &mut visited)
			.await?
			.ok_or_else(
				|| tg::error!(%lock = lock_path.display(), ?search = arg.search, "failed to find node in lock"),
			)
	}

	async fn find_node_in_lock_inner(
		&self,
		mut arg: FindArg<'_>,
		visited: &mut [bool],
	) -> tg::Result<Option<usize>> {
		// If this is the node we're searching for, return.
		match arg.search {
			Either::Left(node) if node == arg.current_node => {
				return Ok(Some(arg.current_node));
			},
			Either::Right(path) if path == arg.current_node_path => {
				return Ok(Some(arg.current_node));
			},
			_ => (),
		}

		// Check if this node has been visited and update the visited set.
		if visited[arg.current_node] {
			return Ok(None);
		}
		visited[arg.current_node] = true;

		match &arg.nodes[arg.current_node] {
			tg::graph::data::Node::Directory(tg::graph::data::Directory { entries, .. }) => {
				// If this is a directory with a root module, update the current package path/node.
				if entries
					.keys()
					.any(|name| tg::package::is_root_module_path(name.as_ref()))
				{
					arg.current_package_path = arg.current_node_path.clone();
					arg.current_package_node = arg.current_node;
				}

				// Recurse over the entries.
				for (name, edge) in entries {
					let current_node_path = arg.current_node_path.join(name);
					let current_node = edge.unwrap_reference_ref().node;
					let arg = FindArg {
						current_node_path,
						current_node,
						..arg.clone()
					};
					let result = Box::pin(self.find_node_in_lock_inner(arg, visited)).await?;
					if let Some(result) = result {
						return Ok(Some(result));
					}
				}
			},

			tg::graph::data::Node::File(tg::graph::data::File { dependencies, .. }) => {
				for (reference, dependency) in dependencies {
					// Skip dependencies that are not contained in the lock.
					let Some(dependency_package_node) = dependency
						.item
						.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node)
					else {
						continue;
					};

					// Skip dependencies contained within the same package, since the traversal is guaranteed to reach them.
					if dependency_package_node == arg.current_package_node {
						continue;
					}

					// Compute the canonical path of the import.
					let path = reference
						.options()
						.local
						.as_ref()
						.or(reference.item().try_unwrap_path_ref().ok())
						.ok_or_else(|| tg::error!(%reference, "expected a path reference"))?;
					let path = arg.current_node_path.parent().unwrap().join(path);
					let path = tokio::fs::canonicalize(&path).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path"),
					)?;

					let current_package_node = dependency_package_node;
					let current_package_path = path.clone();

					// Recurse on the dependency's package.
					let arg = FindArg {
						current_node: current_package_node,
						current_node_path: current_package_path.clone(),
						current_package_node,
						current_package_path,
						..arg.clone()
					};
					let result = Box::pin(self.find_node_in_lock_inner(arg, visited)).await?;

					if let Some(path) = result {
						return Ok(Some(path));
					}
				}
			},

			tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
				artifact,
				path: path_,
			}) => {
				// Get the referent artifact.
				let Some(tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					node: artifact,
					..
				})) = artifact
				else {
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
				let arg = FindArg {
					current_node: current_package_node,
					current_node_path: current_package_path.clone(),
					current_package_node,
					current_package_path,
					..arg.clone()
				};
				let result = Box::pin(self.find_node_in_lock_inner(arg, visited)).await?;
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

impl Lock {
	pub fn get_node_for_path(&self, node_path: &Path) -> tg::Result<usize> {
		self.paths
			.iter()
			.position(|path| path.as_deref() == Some(node_path))
			.ok_or_else(|| tg::error!(%path = node_path.display(), "failed to find in lock"))
	}
}

impl Server {
	pub(crate) fn try_read_lock_for_path(&self, path: &Path) -> tg::Result<Option<Lock>> {
		let contents = 'a: {
			// Read the lock's xattrs.
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

			// Read the lock from disk.
			let lock_path = path.join(tg::package::LOCKFILE_FILE_NAME);
			let contents = match std::fs::read(&lock_path) {
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
						tg::error!(!source, %path = lock_path.display(), "failed to read lock"),
					);
				},
			};
			Some(contents)
		};

		let Some(contents) = contents else {
			return Ok(None);
		};

		// Deserialize the lock.
		let lock = serde_json::from_slice::<tg::graph::Data>(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize lock"),
		)?;

		// Get any referenced objects that are not contained within the lock.
		let mut object_ids: Vec<tg::object::Id> = Vec::new();
		for node in &lock.nodes {
			match node {
				tg::graph::data::Node::Directory(tg::graph::data::Directory {
					entries, ..
				}) => {
					let ids = entries.values().filter_map(|edge| {
						edge.try_unwrap_object_ref().ok().cloned().map(Into::into)
					});
					object_ids.extend(ids);
				},
				tg::graph::data::Node::File(tg::graph::data::File { dependencies, .. }) => {
					let ids = dependencies
						.values()
						.filter_map(|referent| referent.item.try_unwrap_object_ref().ok().cloned());
					object_ids.extend(ids);
				},
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
					artifact: Some(tg::graph::data::Edge::Object(artifact)),
					..
				}) => {
					object_ids.push(artifact.clone().into());
				},
				tg::graph::data::Node::Symlink(_) => (),
			}
		}
		let artifact_ids: Vec<tg::artifact::Id> = object_ids
			.into_iter()
			.map(tg::artifact::Id::try_from)
			.collect::<tg::Result<_>>()?;

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
		// Get the paths for the lock nodes.
		let paths = get_paths(path, &lock)?;

		let objects = Self::create_objects_from_lock(&lock.nodes)
			.map_err(|source| tg::error!(!source, "failed to create objects from lock"))?;

		// Create the parsed lock.
		let lock = Lock {
			nodes: lock.nodes,
			objects,
			paths,
		};

		Ok(Some(lock))
	}
}

impl Server {
	pub(crate) fn create_object_from_lock_node(
		nodes: &[tg::graph::data::Node],
		node: usize,
	) -> tg::Result<tg::Object> {
		Self::create_objects_from_lock(nodes)?[node]
			.clone()
			.ok_or_else(|| tg::error!("failed to create the object"))
	}

	fn create_objects_from_lock(
		nodes: &[tg::graph::data::Node],
	) -> tg::Result<Vec<Option<tg::Object>>> {
		let mut visited = vec![None; nodes.len()];

		let sccs = petgraph::algo::tarjan_scc(&Petgraph { nodes });

		for mut scc in sccs {
			// Create normal objects for existing items.
			if scc.len() == 1 {
				Self::try_create_normal_object_from_lock_node(nodes, scc[0], &mut visited)?;
				continue;
			}
			scc.reverse();
			let graph_indices = scc
				.iter()
				.copied()
				.enumerate()
				.map(|(graph, lock)| (lock, graph))
				.collect::<BTreeMap<_, _>>();

			// Create the graph object.
			let mut graph_nodes = Vec::with_capacity(scc.len());
			for lock_index in &scc {
				Self::try_create_graph_node_from_lock_node(
					nodes,
					*lock_index,
					&graph_indices,
					&mut graph_nodes,
					&mut visited,
				)?;
			}

			// Construct the graph.
			let graph = tg::Graph::with_nodes(graph_nodes);

			// Construct the objects.
			for (graph_index, lock_index) in scc.into_iter().enumerate() {
				let object = match &nodes[lock_index] {
					tg::graph::data::Node::Directory(_) => {
						tg::Directory::with_graph_and_node(graph.clone(), graph_index).into()
					},
					tg::graph::data::Node::File(_) => {
						tg::File::with_graph_and_node(graph.clone(), graph_index).into()
					},
					tg::graph::data::Node::Symlink(_) => {
						tg::Symlink::with_graph_and_node(graph.clone(), graph_index).into()
					},
				};
				visited[lock_index].replace(object);
			}
		}

		Ok(visited)
	}

	fn try_create_graph_node_from_lock_node(
		lock_nodes: &[tg::graph::data::Node],
		lock_index: usize,
		graph_indices: &BTreeMap<usize, usize>,
		graph_nodes: &mut Vec<tg::graph::Node>,
		visited: &mut [Option<tg::Object>],
	) -> tg::Result<()> {
		let node =
			match &lock_nodes[lock_index] {
				tg::graph::data::Node::Directory(directory) => {
					let Ok(entries) = directory
						.entries
						.clone()
						.into_iter()
						.map(|(name, edge)| {
							let edge =
								match edge {
									tg::graph::data::Edge::Reference(
										tg::graph::data::Reference { node, .. },
									) if graph_indices.contains_key(&node) => tg::graph::object::Edge::Reference(
										tg::graph::object::Reference {
											graph: None,
											node: graph_indices.get(&node).copied().unwrap(),
										},
									),
									tg::graph::data::Edge::Reference(
										tg::graph::data::Reference { node, .. },
									) => {
										let object = visited[node]
											.as_ref()
											.ok_or_else(|| tg::error!("expected an object"))?
											.clone()
											.try_into()?;
										tg::graph::object::Edge::Object(object)
									},
									tg::graph::data::Edge::Object(id) => {
										let artifact = tg::Artifact::with_id(id.clone());
										tg::graph::object::Edge::Object(artifact)
									},
								};
							Ok::<_, tg::Error>((name, edge))
						})
						.try_collect()
					else {
						return Ok(());
					};
					let directory = tg::graph::object::Directory { entries };
					tg::graph::Node::Directory(directory)
				},
				tg::graph::data::Node::File(file) => {
					let Some(contents) = file.contents.clone() else {
						return Ok(());
					};
					let Ok(dependencies) =
						file.dependencies
							.clone()
							.into_iter()
							.map(|(reference, referent)| {
								let item = match referent.item() {
									tg::graph::data::Edge::Reference(
										tg::graph::data::Reference { node: index, .. },
									) if graph_indices.contains_key(index) => tg::graph::object::Edge::Reference(
										tg::graph::object::Reference {
											graph: None,
											node: graph_indices.get(index).copied().unwrap(),
										},
									),
									tg::graph::data::Edge::Reference(
										tg::graph::data::Reference { node: index, .. },
									) => {
										let object = visited[*index]
											.as_ref()
											.ok_or_else(|| tg::error!("expected an object"))?
											.clone();
										tg::graph::object::Edge::Object(object)
									},
									tg::graph::data::Edge::Object(id) => {
										let object = tg::Object::with_id(id.clone());
										tg::graph::object::Edge::Object(object)
									},
								};
								let referent = referent.map(|_| item);
								Ok::<_, tg::Error>((reference, referent))
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
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, path }) => {
					let artifact = match artifact {
						Some(tg::graph::data::Edge::Reference(reference))
							if graph_indices.contains_key(&reference.node) =>
						{
							let artifact =
								tg::graph::object::Edge::Reference(tg::graph::object::Reference {
									graph: None,
									node: graph_indices.get(&reference.node).copied().unwrap(),
								});
							Some(artifact)
						},
						Some(tg::graph::data::Edge::Reference(reference)) => {
							let object = visited[reference.node]
								.as_ref()
								.ok_or_else(|| tg::error!("expected an object"))?
								.clone()
								.try_into()?;
							Some(tg::graph::object::Edge::Object(object))
						},
						Some(tg::graph::data::Edge::Object(id)) => {
							let artifact = tg::Artifact::with_id(id.clone());
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

	fn try_create_normal_object_from_lock_node(
		nodes: &[tg::graph::data::Node],
		node: usize,
		visited: &mut [Option<tg::Object>],
	) -> tg::Result<()> {
		let object: tg::Object = match &nodes[node] {
			tg::graph::data::Node::Directory(directory) => {
				let Ok(entries) = directory
					.entries
					.iter()
					.map(|(name, edge)| match edge {
						tg::graph::data::Edge::Reference(reference) => {
							let object = visited[reference.node]
								.clone()
								.ok_or_else(|| tg::error!("expected an object"))?;
							let artifact = tg::Artifact::try_from(object)?;
							Ok::<_, tg::Error>((name.clone(), artifact))
						},
						tg::graph::data::Edge::Object(id) => {
							let artifact = tg::Artifact::with_id(id.clone());
							Ok::<_, tg::Error>((name.clone(), artifact))
						},
					})
					.try_collect()
				else {
					return Ok(());
				};
				tg::Directory::with_entries(entries).into()
			},
			tg::graph::data::Node::File(file) => {
				let Some(contents) = file.contents.clone() else {
					return Ok(());
				};
				let Ok(dependencies) = file
					.dependencies
					.iter()
					.map(|(reference, referent)| {
						let item = match referent.item() {
							tg::graph::data::Edge::Reference(reference) => visited[reference.node]
								.clone()
								.ok_or_else(|| tg::error!("expected an object"))?,
							tg::graph::data::Edge::Object(id) => tg::Object::with_id(id.clone()),
						};
						Ok::<_, tg::Error>((reference.clone(), referent.clone().map(|_| item)))
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
			tg::graph::data::Node::Symlink(symlink) => {
				let artifact = match &symlink.artifact {
					Some(tg::graph::data::Edge::Reference(reference)) => {
						let object = visited[reference.node]
							.clone()
							.ok_or_else(|| tg::error!("expected an object"))?;
						let artifact = object.try_into()?;
						Some(tg::graph::object::Edge::Object(artifact))
					},
					Some(tg::graph::data::Edge::Object(id)) => {
						let artifact = tg::Artifact::with_id(id.clone());
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

// Given a lock, get the paths of all the nodes.
fn get_paths(root_path: &Path, lock: &tg::graph::Data) -> tg::Result<Vec<Option<PathBuf>>> {
	fn get_paths_inner(
		lock: &tg::graph::Data,
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
		match &lock.nodes[node] {
			tg::graph::data::Node::Directory(tg::graph::data::Directory { entries, .. }) => {
				for (name, edge) in entries {
					let tg::graph::data::Edge::Reference(reference) = edge else {
						continue;
					};
					let node_path = node_path.join(name);
					get_paths_inner(lock, &node_path, reference.node, visited)?;
				}
			},
			tg::graph::data::Node::File(tg::graph::data::File { dependencies, .. }) => {
				for referent in dependencies.values() {
					let tg::graph::data::Edge::Reference(reference) = referent.item() else {
						continue;
					};
					// Skip tags.
					if referent.tag().is_some() {
						continue;
					}
					let Some(path) = referent.path() else {
						continue;
					};
					let path = node_path.parent().unwrap().join(path);
					if !matches!(path.try_exists(), Ok(true)) {
						continue;
					}
					let Ok(node_path) = crate::util::fs::canonicalize_parent_sync(&path) else {
						continue;
					};
					get_paths_inner(lock, &node_path, reference.node, visited)?;
				}
			},
			tg::graph::data::Node::Symlink(_) => (),
		}

		Ok(())
	}

	if lock.nodes.is_empty() {
		return Ok(Vec::new());
	}

	let mut visited = vec![None; lock.nodes.len()];
	let node = 0;
	get_paths_inner(lock, root_path, node, &mut visited)?;
	Ok(visited)
}

pub fn strip(
	old_nodes: &[tg::graph::data::Node],
	object_ids: &[Option<tg::object::Id>],
	root: usize,
) -> Vec<tg::graph::data::Node> {
	let preserve = mark(old_nodes);

	// Strip nodes that do not reference tag dependencies.
	let mut new_nodes = Vec::with_capacity(old_nodes.len());
	let mut visited = vec![None; old_nodes.len()];
	strip_inner(
		old_nodes,
		root,
		&mut visited,
		&mut new_nodes,
		object_ids,
		&preserve,
	);

	new_nodes.into_iter().map(Option::unwrap).collect()
}

// Marks nodes if they or any of their transitive children are eligible for deletion.
fn mark(nodes: &[tg::graph::data::Node]) -> Vec<bool> {
	// Add the child to the same set as the parent.
	fn union(set: &mut [usize], parent: usize, child: usize) {
		set[child] = find(set, parent);
	}

	// Find the characteristic set of an item.
	fn find(set: &mut [usize], item: usize) -> usize {
		let mut current = item;
		loop {
			if set[current] == current {
				set[item] = current;
				return current;
			}
			current = set[current];
		}
	}

	let sccs = petgraph::algo::tarjan_scc(&Petgraph { nodes });

	// Mark tagged items.
	let mut set = (0..=nodes.len()).collect::<Vec<_>>();
	let tagged = nodes.len();

	// First mark nodes to tag. This is done in a separate pass because it operates top-down.
	for scc in &sccs {
		for parent in scc.iter().copied() {
			match &nodes[parent] {
				tg::graph::data::Node::Directory(directory) => {
					for child in directory.entries.values().filter_map(|edge| {
						edge.try_unwrap_reference_ref()
							.ok()
							.map(|reference| reference.node)
					}) {
						// Mark directory children as part of the same set as their parent.
						union(&mut set, parent, child);
					}
				},
				tg::graph::data::Node::File(file) => {
					for (child, is_tagged) in
						file.dependencies
							.iter()
							.filter_map(|(reference, referent)| {
								let child = referent
									.item
									.try_unwrap_reference_ref()
									.ok()
									.map(|reference| reference.node)?;
								let is_tagged = reference.item().try_unwrap_tag_ref().is_ok()
									&& (reference.options().local.is_none()
										&& !reference.item().is_path());
								Some((child, is_tagged))
							}) {
						if is_tagged {
							// If a dependency is tagged, add it to the set of tagged nodes.
							union(&mut set, tagged, child);
						} else {
							// Otherwise, mark it as part of the same set as the parent.
							union(&mut set, parent, child);
						}
					}
				},
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
					artifact:
						Some(tg::graph::data::Edge::Reference(tg::graph::data::Reference {
							node: child,
							..
						})),
					..
				}) => {
					union(&mut set, parent, *child);
				},
				tg::graph::data::Node::Symlink(_) => (),
			}
		}
	}

	// Now, determine which nodes to preserve.
	let mut preserve = vec![false; nodes.len()];
	for scc in sccs {
		// Next, check if any dependencies are marked.
		for node in scc.iter().copied() {
			let has_dependencies = match &nodes[node] {
				tg::graph::data::Node::Directory(directory) => directory
					.entries
					.values()
					.filter_map(|edge| {
						edge.try_unwrap_reference_ref()
							.ok()
							.map(|reference| reference.node)
					})
					.any(|node| preserve[node] || find(&mut set, node) == tagged),
				tg::graph::data::Node::File(file) => {
					file.dependencies.iter().any(|(reference, referent)| {
						(reference.item().try_unwrap_tag_ref().is_ok()
							&& (reference.options().local.is_none() && !reference.item().is_path()))
							|| referent
								.item
								.try_unwrap_reference_ref()
								.ok()
								.map(|reference| reference.node)
								.is_some_and(|item| {
									preserve[item] || find(&mut set, node) == tagged
								})
					})
				},
				tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
					artifact:
						Some(tg::graph::data::Edge::Reference(tg::graph::data::Reference {
							node, ..
						})),
					..
				}) => preserve[*node] || find(&mut set, *node) == tagged,
				tg::graph::data::Node::Symlink(_) => false,
			};
			preserve[node] = has_dependencies;
		}
	}

	preserve
}

fn strip_inner(
	old_nodes: &[tg::graph::data::Node],
	node: usize,
	visited: &mut Vec<Option<usize>>,
	new_nodes: &mut Vec<Option<tg::graph::data::Node>>,
	object_ids: &[Option<tg::object::Id>],
	preserve: &[bool],
) -> Option<tg::graph::data::Edge<tg::object::Id>> {
	if !preserve[node] {
		return object_ids[node].clone().map(tg::graph::data::Edge::Object);
	}

	if let Some(visited) = visited[node] {
		return Some(tg::graph::data::Edge::Reference(
			tg::graph::data::Reference {
				graph: None,
				node: visited,
			},
		));
	}

	let new_node = new_nodes.len();
	visited[node].replace(new_node);
	new_nodes.push(None);

	match old_nodes[node].clone() {
		tg::graph::data::Node::Directory(directory) => {
			let entries = directory
				.entries
				.into_iter()
				.filter_map(|(name, edge)| {
					let edge = match edge {
						tg::graph::data::Edge::Reference(reference) => strip_inner(
							old_nodes,
							reference.node,
							visited,
							new_nodes,
							object_ids,
							preserve,
						)
						.map(|edge| match edge {
							tg::graph::data::Edge::Reference(reference) => {
								tg::graph::data::Edge::Reference(reference)
							},
							tg::graph::data::Edge::Object(object) => {
								tg::graph::data::Edge::Object(object.try_into().unwrap())
							},
						})?,
						tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id),
					};
					Some((name, edge))
				})
				.collect();

			// Create a new node.
			let directory = tg::graph::data::Directory { entries };
			new_nodes[new_node].replace(tg::graph::data::Node::Directory(directory));
		},
		tg::graph::data::Node::File(file) => {
			let dependencies = file
				.dependencies
				.into_iter()
				.filter_map(|(reference, referent)| {
					// Special case, the reference is by ID.
					let item: tg::graph::data::Edge<tg::object::Id> = match referent.item() {
						tg::graph::data::Edge::Reference(reference_) => {
							if let Some(node) = strip_inner(
								old_nodes,
								reference_.node,
								visited,
								new_nodes,
								object_ids,
								preserve,
							) {
								node
							} else if let Ok(id) = reference.item().try_unwrap_object_ref() {
								tg::graph::data::Edge::Object(id.clone())
							} else {
								return None;
							}
						},
						tg::graph::data::Edge::Object(id) => {
							tg::graph::data::Edge::Object(id.clone())
						},
					};
					Some((reference, referent.map(|_| item)))
				})
				.collect();

			// Retain the contents if this is not a path dependency.
			let contents = if object_ids[node].is_none() {
				None
			} else {
				file.contents
			};

			// Create the node.
			let file = tg::graph::data::File {
				contents,
				dependencies,
				executable: file.executable,
			};
			new_nodes[new_node].replace(tg::graph::data::Node::File(file));
		},

		tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, path }) => {
			// Remap the artifact if necessary.
			let artifact = match artifact {
				Some(tg::graph::data::Edge::Reference(reference)) => strip_inner(
					old_nodes,
					reference.node,
					visited,
					new_nodes,
					object_ids,
					preserve,
				)
				.map(|edge| match edge {
					tg::graph::data::Edge::Reference(reference) => {
						tg::graph::data::Edge::Reference(reference)
					},
					tg::graph::data::Edge::Object(object) => {
						tg::graph::data::Edge::Object(object.try_into().unwrap())
					},
				}),
				Some(tg::graph::data::Edge::Object(id)) => Some(tg::graph::data::Edge::Object(id)),
				None => None,
			};

			// Create the node.
			new_nodes[new_node].replace(tg::graph::data::Node::Symlink(tg::graph::data::Symlink {
				artifact,
				path,
			}));
		},
	}

	Some(tg::graph::data::Edge::Reference(
		tg::graph::data::Reference {
			graph: None,
			node: new_node,
		},
	))
}

struct Petgraph<'a> {
	nodes: &'a [tg::graph::data::Node],
}

impl petgraph::visit::GraphBase for Petgraph<'_> {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl petgraph::visit::IntoNodeIdentifiers for &Petgraph<'_> {
	type NodeIdentifiers = std::ops::Range<usize>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

impl petgraph::visit::NodeIndexable for Petgraph<'_> {
	fn node_bound(&self) -> usize {
		self.nodes.len()
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}

	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &Petgraph<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		match &self.nodes[id] {
			tg::graph::data::Node::Directory(directory) => directory
				.entries
				.values()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node)
				})
				.boxed(),
			tg::graph::data::Node::File(file) => file
				.dependencies
				.values()
				.filter_map(|referent| {
					referent
						.item
						.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node)
				})
				.boxed(),
			tg::graph::data::Node::Symlink(tg::graph::data::Symlink { artifact, .. }) => artifact
				.iter()
				.filter_map(|edge| {
					edge.try_unwrap_reference_ref()
						.ok()
						.map(|reference| reference.node)
				})
				.boxed(),
		}
	}
}
