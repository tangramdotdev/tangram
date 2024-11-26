use crate::Server;
use futures::{stream::FuturesUnordered, Future, FutureExt, TryStreamExt as _};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet, VecDeque},
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_ignore::Ignore;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Graph {
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug)]
pub struct Node {
	pub arg: tg::artifact::checkin::Arg,
	pub data: Option<tg::file::Data>,
	pub edges: Vec<Edge>,
	pub lockfile: Option<(Arc<Lockfile>, usize)>,
	pub metadata: std::fs::Metadata,
	pub parent: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub node: Option<usize>,
	pub object: Option<tg::object::Id>,
	pub path: Option<PathBuf>,
	pub tag: Option<tg::Tag>,
}

#[derive(Clone, Debug)]
pub struct Lockfile {
	pub graphs: BTreeMap<tg::graph::Id, tg::graph::Data>,
	pub nodes: Vec<tg::lockfile::Node>,
	pub path: PathBuf,
	pub objects: BTreeMap<tg::object::Id, tg::object::Data>,
}

struct State {
	ignore: Ignore,
	roots: Vec<usize>,
	graph: Graph,
	visited: BTreeMap<PathBuf, usize>,
}

impl Server {
	pub(super) async fn create_input_graph(
		&self,
		mut arg: tg::artifact::checkin::Arg,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Graph> {
		// Find the root path.
		arg.path = self.find_root(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path.display(), "failed to find root path for checkin"),
		)?;

		// Create the ignore tree.
		let ignore = self.ignore_for_checkin().await?;

		// Initialize state.
		let state = RwLock::new(State {
			ignore,
			roots: Vec::new(),
			visited: BTreeMap::new(),
			graph: Graph { nodes: Vec::new() },
		});

		// Create the graph.
		self.create_input_graph_inner(None, arg.path.as_ref(), &arg, &state, progress)
			.await?;
		
		// Pick lockfiles for each node.
		self.select_lockfiles(&state).await?;

		// Fill in the missing file edges.
		self
			.infer_file_edges(&state)
			.await?;

		// Validate the input graph.
		let State { mut graph, .. } = state.into_inner();
		graph.validate().await?;
		
		// Compute the edges of any files that require lockfiles.
		// self.get_file_data_edges(&mut graph).await?;

		Ok(graph)
	}

	async fn find_root(&self, path: &Path) -> tg::Result<PathBuf> {
		if !path.is_absolute() {
			return Err(tg::error!(%path = path.display(), "expected an absolute path"));
		}

		if !tg::package::is_module_path(path) {
			return Ok(path.to_owned());
		}

		// Look for a `tangram.ts`.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let path = path.parent().unwrap();
		for path in path.ancestors() {
			for root_module_name in tg::package::ROOT_MODULE_FILE_NAMES {
				let root_module_path = path.join(root_module_name);
				if tokio::fs::try_exists(&root_module_path).await.map_err(|source| tg::error!(!source, %root_module_path = root_module_path.display(), "failed to check if root module exists"))? {
					return Ok(path.to_owned());
				}
			}
		}
		drop(permit);

		// Otherwise return the parent.
		Ok(path.to_owned())
	}

	async fn create_input_graph_inner(
		&self,
		referrer: Option<usize>,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<usize> {
		// Get the full path.
		let path = if path.is_absolute() {
			// If the absolute path is provided, use it.
			path.to_owned()
		} else {
			let referrer = referrer.ok_or_else(|| tg::error!("expected a referrer"))?;
			let referrer_path = state.read().await.graph.nodes[referrer].arg.path.clone();
			referrer_path.join(path)
		};

		// Canonicalize the path.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let absolute_path = crate::util::fs::canonicalize_parent(&path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the parent"),
		)?;
		drop(permit);

		// Get the file system metadata.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let metadata = tokio::fs::symlink_metadata(&absolute_path).await.map_err(
			|source| tg::error!(!source, %path = absolute_path.display(), "failed to get file system metadata"),
		)?;
		drop(permit);

		// Validate.
		if !(metadata.is_dir() || metadata.is_file() || metadata.is_symlink()) {
			return Err(tg::error!(%path = absolute_path.display(), "invalid file type"));
		}

		// If this is a module file, ensure the parent is collected.
		if metadata.is_file() && tg::package::is_module_path(&absolute_path) {
			let parent = absolute_path.parent().unwrap().to_owned();
			Box::pin(self.create_input_graph_inner(None, &parent, arg, state, progress))
				.await
				.map_err(
					|source| tg::error!(!source, %path = absolute_path.display(), "failed to collect input of parent"),
				)?;
		}

		// Get a write lock on the state to avoid a race condition.
		let mut state_ = state.write().await;

		// Check if this path has already been visited and return it.
		if let Some(node) = state_.visited.get(&absolute_path) {
			return Ok(*node);
		}

		// Look up the parent if it exists.
		let parent = absolute_path
			.parent()
			.and_then(|parent| state_.visited.get(parent).copied());

		// If this is a file, attempt to get its data from the xattr.
		let data = if metadata.is_file() {
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let xattr_data = xattr::get(&absolute_path, tg::file::XATTR_DATA_NAME)
				.map_err(|source| tg::error!(!source, %path = absolute_path.display(), 
				"failed to get data from xattr"))?;
			xattr_data.map(|data| tg::file::Data::deserialize(&data.into()))
				.transpose()
				.map_err(|source| tg::error!(!source, "failed to deserialize file data"))?
		} else {
			None
		};

		// Create a new node.
		let arg_ = tg::artifact::checkin::Arg {
			path: absolute_path.clone(),
			..arg.clone()
		};
		let node = Node {
			arg: arg_,
			data,
			edges: Vec::new(),
			lockfile: None,
			metadata: metadata.clone(),
			parent,
		};
		state_.graph.nodes.push(node);
		let node = state_.graph.nodes.len() - 1;
		state_.visited.insert(absolute_path.clone(), node);

		// Update the roots if necessary. This may result in dangling directories that also need to be collected.
		let dangling = if parent.is_none() {
			state_.add_root_and_return_dangling_directories(node).await
		} else {
			Vec::new()
		};

		// Release the state.
		drop(state_);

		// Collect any dangling directories.
		for dangling in dangling {
			Box::pin(self.create_input_graph_inner(None, &dangling, arg, state, progress))
				.await
				.map_err(
					|source| tg::error!(!source, %path = dangling.display(), "failed to collect dangling directory"),
				)?;
		}

		// Get the edges.
		let edges = self
			.get_edges(node, &absolute_path, arg, state, progress)
			.await
			.map_err(
				|source| tg::error!(!source, %path = absolute_path.display(), "failed to get edges"),
			)?;

		// Update the node.
		state.write().await.graph.nodes[node].edges = edges;

		// Return the created node.
		Ok(node)
	}

	async fn get_edges(
		&self,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		let metadata = state.read().await.graph.nodes[referrer].metadata.clone();
		if metadata.is_dir() {
			Box::pin(self.get_directory_edges(referrer, path, arg, state, progress)).await
		} else if metadata.is_file() {
			Box::pin(self.get_file_edges(referrer, path, arg, state, progress)).await
		} else if metadata.is_symlink() {
			Box::pin(self.get_symlink_edges(referrer, path, arg, state, progress)).await
		} else {
			Err(tg::error!("invalid file type"))
		}
	}

	async fn get_directory_edges(
		&self,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		// Get the directory entries.
		let mut names = Vec::new();
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let mut read_dir = tokio::fs::read_dir(&path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the directory entries"),
		)?;
		loop {
			let Some(entry) = read_dir.next_entry().await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to get directory entry"),
			)?
			else {
				break;
			};
			let name = entry.file_name().into_string().map_err(
				|_| tg::error!(%path = path.display(), "directory contains entries with non-utf8 children"),
			)?;
			if arg.ignore {
				let file_type = entry
					.file_type()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the file type"))?;
				let is_directory = file_type.is_dir();
				let ignored = state
					.write()
					.await
					.ignore
					.matches(&path.join(&name), Some(is_directory))
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to check if the path should be ignored")
					})?;
				if ignored {
					continue;
				}
			}
			names.push(name);
		}
		drop(read_dir);
		drop(permit);

		let vec: Vec<_> = names
			.into_iter()
			.map(|name| {
				async move {
					let path = path.join(&name);

					// Follow the edge.
					let node = Box::pin(self.create_input_graph_inner(
						Some(referrer),
						name.as_ref(),
						arg,
						state,
						progress,
					))
					.await
					.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to collect child input"),
					)?;

					// Create the edge.
					let reference = tg::Reference::with_path(&name);
					let edge = Edge {
						reference,
						subpath: None,
						node: Some(node),
						object: None,
						path: None,
						tag: None,
					};
					Ok::<_, tg::Error>(edge)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Update the parents of any children.
		let mut state = state.write().await;
		for edge in &vec {
			let Some(node) = edge.node else {
				continue;
			};
			state.graph.nodes[node].parent.replace(referrer);
			if let Some(root_index) = state.roots.iter().position(|root| *root == node) {
				state.roots.remove(root_index);
			}
		}
		drop(state);
		Ok(vec)
	}

	async fn get_file_edges(
		&self,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		// If the file has data set, skip scanning its edges. They will be collected later.
		if state.read().await.graph.nodes[referrer].data.is_some() {
			return Ok(Vec::new());
		}

		// If this is a module path, attempt to parse and get its edges.
		if tg::package::is_module_path(path) {
			return self
				.get_module_edges(referrer, path, arg, state, progress)
				.await;
		}

		Ok(Vec::new())
	}

	async fn get_module_edges(
		&self,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let text = tokio::fs::read_to_string(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to read file contents"),
		)?;
		drop(permit);
		let analysis = match crate::compiler::Compiler::analyze_module(text) {
			Ok(analysis) => analysis,
			Err(error) => {
				// If analyzing the module fails, report a diagnostic and continue.
				if let Some(progress) = progress {
					let diagnostic = tg::Diagnostic {
						location: None, // todo: compute a meaningful location.
						severity: tg::diagnostic::Severity::Error,
						message: format!("failed to analyze the module: {error}"),
					};
					progress.diagnostic(diagnostic);
				}
				return Ok(Vec::new())
			}
		};

		let edges = analysis
			.imports
			.into_iter()
			.map(|import| {
				let arg = arg.clone();
				async move {
					// Follow path dependencies.
					let import_path = import
						.reference
						.item()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| import.reference.options()?.path.as_ref());
					if let Some(import_path) = import_path {
						// Create the reference.
						let reference = import.reference.clone();

						// Check if the import points outside the package.
						let root_node = get_root_node(&state.read().await.graph, referrer)
							.await;
						let root_path = state.read().await.graph.nodes[root_node]
							.arg
							.path
							.clone();

						let absolute_path = crate::util::fs::canonicalize_parent(path.parent().unwrap().join(import_path)).await.map_err(|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path's parent"))?;
						let is_external = absolute_path.strip_prefix(&root_path).is_err();

						// If the import is of a module and points outside the root, return an error.
						if (import_path.is_absolute() ||
							is_external) && tg::package::is_module_path(import_path.as_ref()) {
							return Err(
								tg::error!(%root = root_path.display(), %path = import_path.display(), "cannot import module outside of the package"),
							);
						}

						// Get the input of the referent.
						let child = if is_external {
							// If this is an external import, treat it as a root using the absolute path.
							self.create_input_graph_inner(Some(referrer), &absolute_path, &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						} else {
							// Otherwise, treat it as a relative path.

							// Get the parent of the referrer.
							let parent = state.read().await.graph.nodes[referrer].parent.ok_or_else(|| tg::error!("expected a parent"))?;

							// Recurse.
							self.create_input_graph_inner(Some(parent), import_path.as_ref(), &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						};

						// If the child is a package and the import kind is not directory, get the subpath.
						let (child_path, is_directory) = {
							let state = state.read().await;
							(state.graph.nodes[child].arg.path.clone(), state.graph.nodes[child].metadata.is_dir())
						};
						let (node, subpath) = if is_directory {
							if matches!(&import.kind, Some(tg::module::Kind::Directory)) {
								(child, None)
							} else {
								let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
								let subpath = tg::package::try_get_root_module_file_name(self,Either::Right(&child_path)).await.map_err(|source| tg::error!(!source, %path = child_path.display(), "failed to get root module file name"))?
									.map(PathBuf::from);
								(child, subpath)
							}
						} else {
							let (node, subpath) = root_node_with_subpath(&state.read().await.graph, child).await;

							// Sanity check.
							if tg::package::is_module_path(&child_path) && subpath.is_none() {
								return Err(tg::error!(%path = child_path.display(), "expected a subpath"));
							}

							(node, subpath)
						};

						// Create the edge.
						let package_path = state.read().await.graph.nodes[node].arg.path.clone();
						let path = crate::util::path::diff(&arg.path, &package_path)?;
						let edge = Edge {
							reference,
							node: Some(node),
							object: None,
							path: Some(path),
							subpath,
							tag: None,
						};

						return Ok(edge);
					}

					Ok(Edge {
						reference: import.reference,
						node: None,
						object: None,
						path: None,
						subpath: None,
						tag: None,
					})
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		Ok(edges)
	}

	async fn get_symlink_edges(
		&self,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		_progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		// Check if this node's edges have already been created.
		let existing = state.read().await.graph.nodes[referrer].edges.clone();
		if !existing.is_empty() {
			return Ok(existing);
		}

		// Read the symlink.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let target = tokio::fs::read_link(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to read symlink"),
		)?;
		drop(permit);

		// Error if this is an absolute path or empty.
		if target.is_absolute() {
			return Err(
				tg::error!(%path = path.display(), %target = target.display(), "cannot check in an absolute path symlink"),
			);
		} else if target.as_os_str().is_empty() {
			return Err(
				tg::error!(%path = path.display(),  "cannot check in a path with an empty target"),
			);
		}

		// Get the full target by joining with the parent.
		let target_absolute_path = crate::util::fs::canonicalize_parent(
			path.parent().unwrap().join(&target),
		)
		.await
		.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path's parent"),
		)?;

		// Check if this is a checkin of an artifact.
		let artifacts_path = self.get_artifacts_path_for_path(&path).await?;
		let path = crate::util::path::diff(&arg.path, &target_absolute_path)?;
		let edge = if let Ok(subpath) = target_absolute_path.strip_prefix(artifacts_path) {
			let mut components = subpath.components();
			let object = components
				.next()
				.ok_or_else(
					|| tg::error!(%subpath = subpath.display(), "expected a subpath component"),
				)?
				.as_os_str()
				.to_str()
				.ok_or_else(|| tg::error!(%subpath = subpath.display(), "invalid subpath"))?
				.parse()
				.map_err(|_| tg::error!(%subpath = subpath.display(), "expected an object id"))?;
			let subpath = components.collect();
			Edge {
				reference: tg::Reference::with_path(target),
				subpath: Some(subpath),
				node: None,
				object: Some(object),
				path: Some(path),
				tag: None,
			}
		} else {
			Edge {
				reference: tg::Reference::with_path(target),
				subpath: None,
				node: None,
				object: None,
				path: Some(path),
				tag: None,
			}
		};
		Ok(vec![edge])
	}

	async fn get_artifacts_path_for_path(&self, path: &Path) -> tg::Result<PathBuf> {
		let mut artifacts_path = None;

		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		for ancestor in path.ancestors().skip(1) {
			let path = ancestor.join(".tangram/artifacts");
			if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
				artifacts_path.replace(path);
				break;
			}
		}
		Ok(artifacts_path.unwrap_or(self.artifacts_path()))
	}
}

impl Server {
	async fn select_lockfiles(&self, state: &RwLock<State>) -> tg::Result<()> {
		let mut state = state.write().await;
		let visited = RwLock::new(BTreeSet::new());
		self.select_lockfiles_inner(&mut state.graph, 0, &visited).await?;
		Ok(())
	}

	async fn select_lockfiles_inner(
		&self,
		graph: &mut Graph,
		node: usize,
		visited: &RwLock<BTreeSet<usize>>,
	) -> tg::Result<()> {
		// Check if this path is visited or not.
		if visited.read().await.contains(&node) {
			return Ok(());
		}

		// Mark this node as visited.
		visited.write().await.insert(node);

		// If this is not a root module, inherit the lockfile of the referrer.
		let path = graph.nodes[node].arg.path.clone();
		let lockfile = if tg::package::is_root_module_path(path.as_ref()) {
			// Try and find a lockfile.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			if let Ok(Some((lockfile, path, node))) = crate::lockfile::try_get_lockfile_node_for_module_path(path.as_ref())
				.await {
				let crate::lockfile::Objects { graphs, objects } = crate::lockfile::get_objects(&lockfile)
					.map_err(|source| tg::error!(!source, %path = path.display(), "invalid lockfile"))?;
				let graphs = graphs
					.into_iter()
					.map(|data| {
						let id = tg::graph::Id::new(&data.serialize()?);
						Ok::<_, tg::Error>((id, data))
					}).try_collect()?;
				let objects = objects
					.into_iter()
					.flatten()
					.map(|data| {
						let id = tg::object::Id::new(data.kind(), &data.serialize()?);
						Ok::<_, tg::Error>((id, data))
					})
					.try_collect()?;
				let lockfile = Lockfile {
					nodes: lockfile.nodes,
					graphs,
					path,
					objects
				};
				Some((Arc::new(lockfile), node))
			} else {
				None
			} 
		} else {
			// Otherwise inherit from the referrer.
			let parent = graph.nodes[node].parent;
			if let Some(parent) = parent {
				graph.nodes[parent].lockfile.clone()
			} else {
				None
			}
		};
		graph.nodes[node].lockfile = lockfile;

		// Recurse.
		let edges = graph.nodes[node].edges.clone();
		for edge in edges {
			let Some(node) = edge.node else {
				continue;
			};
			Box::pin(self.select_lockfiles_inner(graph, node, visited)).await?;
		}
		Ok(())
	}
}

impl Server {
	async fn infer_file_edges(&self, arg: &tg::artifact::checkin::Arg, state: &RwLock<State>) -> tg::Result<()> {
		// Find all the files with xattrs.
		let roots = state
			.read()
			.await
			.graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, node)| {
				node.data.is_some().then_some(index)
			})
			.collect::<Vec<_>>();

		// Recurse.
		for root in roots {
			let node = state.read().await.graph.nodes[root].clone();
			let path = node.arg.path.clone();
			let (lockfile, _) = node.lockfile.clone().ok_or_else(|| tg::error!("expected a lockfile"))?;
			let referrer = Some(root);
			Box::pin(self.infer_file_edges_inner(referrer, state, path, lockfile, arg)).await?;
		}

		Ok(())
	}

	async fn infer_file_edges_inner(
		&self, 
		referrer: Option<usize>,
		state: &RwLock<State>,
		path: PathBuf, 
		lockfile: Arc<Lockfile>,
		arg: &tg::artifact::checkin::Arg,
	) -> tg::Result<usize> {
		// Check if this node has been visited.
		let mut state_ = state.write().await;
		let lockfile_node = if let Some(index) = state_.visited.get(&path).copied() {
			if state_.graph.nodes[index].data.is_none() &&
				!state_.graph.nodes[index].edges.is_empty() {
					return Ok(index)
				}
			state_.graph
				.nodes
				[index]
				.lockfile
				.ok_or_else(|| tg::error!("incomplete input graph"))?
				.1
		} else {
			// Find the path in the lockfile.

			// Get the object file data

			// Get the file system metadata

			// 
			todo!()
		}
		match state_.visited.entry(path.clone()) {
			std::collections::btree_map::Entry::Occupied(occupied) {
				let index = *occupied.get();
				let node = &state_.graph.nodes[index];
				if node.data.is_none() || node.edges.is_empty() {
					return Ok(index)
				}
			},
			std::collections::btree_map::Entry::Vacant(vacant) => {
				let (node, data) = lockfile.find_node(&path)?;
				let node = Node {
					arg: tg::artifact::checkin::Arg {
						path: path.clone(),
						..arg.clone()
					},
					data,
					edges: Vec::new(),
					lockfile: Some(Lockfile.clone(), node),
					metadata,
					parent,
				};
			},
		}


		todo!()
	}
}
// #[derive(Clone, Debug)]
// struct LockfileNodeData {
// 	pub id: tg::artifact::Id,
// 	pub data: tg::artifact::Data,
// }

// impl Server {
// 	async fn collect_dependencies(&self, input: &mut Graph) -> tg::Result<()> {
// 		// Collect the ids of
// 		for node in &input.nodes {
// 			let Some((lockfile, _)) = &node.lockfile else {
// 				continue;
// 			};
// 			let explicit = self.get_explicit_dependenices(lockfile.as_ref()).await?;
// 			let implicit = self.get_implicit_dependencies(lockfile.as_ref()).await?;
// 		}
// 		todo!()
// 	}

// 	async fn get_explicit_dependenices(
// 		&self,
// 		lockfile: &tg::Lockfile,
// 	) -> tg::Result<BTreeSet<tg::artifact::Id>> {
// 		// Get all the directory referenced dependencies.
// 		let mut ids = Vec::new();
// 		for node in &lockfile.nodes {
// 			match node {
// 				tg::lockfile::Node::Directory { entries } => {
// 					let it = entries
// 						.values()
// 						.filter_map(|entry| entry.as_ref().right().cloned());
// 					ids.extend(it);
// 				},
// 				tg::lockfile::Node::File { dependencies, .. } => {
// 					let it = dependencies
// 						.values()
// 						.filter_map(|referent| referent.item.as_ref().right().cloned());
// 					ids.extend(it);
// 				},
// 				tg::lockfile::Node::Symlink { artifact, .. } => {
// 					let it = artifact
// 						.as_ref()
// 						.and_then(|artifact| artifact.as_ref().right().cloned())
// 						.into_iter();
// 					ids.extend(it);
// 				},
// 			}
// 		}
// 		let ids = ids
// 			.into_iter()
// 			.filter_map(|id| match id {
// 				tg::object::Id::Directory(artifact) => Some(artifact.into()),
// 				tg::object::Id::File(artifact) => Some(artifact.into()),
// 				tg::object::Id::Symlink(artifact) => Some(artifact.into()),
// 				_ => None,
// 			})
// 			.collect();
// 		Ok(ids)
// 	}

// 	async fn get_implicit_dependencies(
// 		&self,
// 		lockfile: &tg::Lockfile,
// 	) -> tg::Result<Vec<LockfileNodeData>> {
// 		let mut data = vec![None; lockfile.nodes.len()];

// 		// Split into strongly connected components.
// 		for mut scc in tarjan_scc(&LockfileGraphImpl(lockfile)) {
// 			scc.reverse();

// 			// Assign a graph index for each lockfile node.
// 			let graph_indices = scc
// 				.iter()
// 				.copied()
// 				.enumerate()
// 				.map(|(graph_index, lockfile_index)| (lockfile_index, graph_index))
// 				.collect::<BTreeMap<_, _>>();

// 			// Skip SCCs that area missing file contents.
// 			let is_local = scc.iter().copied().all(|node| {
// 				let tg::lockfile::Node::File { contents, .. } = &lockfile.nodes[node] else {
// 					return true;
// 				};
// 				contents.is_some()
// 			});
// 			if is_local {
// 				continue;
// 			}

// 			// If the SCC is of length 1, it represents a normal artifact.
// 			if scc.len() == 1 {
// 				let node_data = match &lockfile.nodes[scc[0]] {
// 					tg::lockfile::Node::Directory { entries } => {
// 						let entries = entries
// 							.iter()
// 							.map(|(name, entry)| {
// 								let entry = try_convert_to_graph_edge(&graph_indices, entry)?
// 									.right()
// 									.ok_or_else(|| tg::error!("invalid lockfile"))?;
// 								Ok::<_, tg::Error>((name.clone(), entry))
// 							})
// 							.try_collect()?;
// 						let data = tg::directory::Data::Normal { entries };
// 						let id = tg::directory::Id::new(&data.serialize()?).into();
// 						let data = tg::artifact::Data::Directory(data);
// 						LockfileNodeData { id, data }
// 					},
// 					tg::lockfile::Node::File {
// 						contents,
// 						dependencies,
// 						executable,
// 					} => {
// 						let contents = contents
// 							.clone()
// 							.ok_or_else(|| tg::error!("invalid lockfile"))?;
// 						let dependencies = dependencies
// 							.iter()
// 							.map(|(reference, referent)| {
// 								let item = referent
// 									.item
// 									.clone()
// 									.right()
// 									.ok_or_else(|| tg::error!("invalid lockfile"))?;
// 								let referent = tg::Referent {
// 									item,
// 									path: referent.path.clone(),
// 									subpath: referent.subpath.clone(),
// 									tag: referent.tag.clone(),
// 								};
// 								Ok::<_, tg::Error>((reference.clone(), referent))
// 							})
// 							.try_collect()?;
// 						let executable = *executable;
// 						let data = tg::file::Data::Normal {
// 							contents,
// 							dependencies,
// 							executable,
// 						};
// 						let id = tg::file::Id::new(&data.serialize()?).into();
// 						let data = tg::artifact::Data::File(data);
// 						LockfileNodeData { id, data }
// 					},
// 					tg::lockfile::Node::Symlink { artifact, subpath } => {
// 						let artifact = artifact
// 							.as_ref()
// 							.map(|artifact| {
// 								let artifact = try_convert_to_graph_edge(&graph_indices, artifact)?;
// 								artifact
// 									.right()
// 									.ok_or_else(|| tg::error!("invalid lockfile"))
// 							})
// 							.transpose()?;
// 						let subpath = subpath.clone();
// 						let data = tg::symlink::Data::Normal { artifact, subpath };
// 						let id = tg::symlink::Id::new(&data.serialize()?).into();
// 						let data = tg::artifact::Data::Symlink(data);
// 						LockfileNodeData { id, data }
// 					},
// 				};
// 				data[scc[0]].replace(node_data);
// 				continue;
// 			}

// 			// Recover the graph nodes from the lockfile.
// 			let mut nodes = Vec::with_capacity(scc.len());
// 			for index in scc.iter().copied() {
// 				let node = match &lockfile.nodes[index] {
// 					tg::lockfile::Node::Directory { entries } => {
// 						let entries = entries
// 							.iter()
// 							.map(|(name, entry)| {
// 								let entry = try_convert_to_graph_edge(&graph_indices, &entry)?;
// 								Ok::<_, tg::Error>((name.clone(), entry))
// 							})
// 							.try_collect()?;
// 						let data = tg::graph::data::Directory { entries };
// 						tg::graph::data::Node::Directory(data)
// 					},
// 					tg::lockfile::Node::File {
// 						contents,
// 						dependencies,
// 						executable,
// 					} => {
// 						let contents = contents
// 							.clone()
// 							.ok_or_else(|| tg::error!("invalid lockfile"))?;
// 						let dependencies = dependencies
// 							.iter()
// 							.map(|(reference, referent)| {
// 								let item = referent
// 									.item
// 									.clone()
// 									.map_left(|index| graph_indices.get(&index).copied().unwrap());
// 								let reference = reference.clone();
// 								let referent = tg::Referent {
// 									item,
// 									path: referent.path.clone(),
// 									subpath: referent.subpath.clone(),
// 									tag: referent.tag.clone(),
// 								};
// 								Ok::<_, tg::Error>((reference, referent))
// 							})
// 							.try_collect()?;
// 						let executable = *executable;
// 						let data = tg::graph::data::File {
// 							contents,
// 							dependencies,
// 							executable,
// 						};
// 						tg::graph::data::Node::File(data)
// 					},
// 					tg::lockfile::Node::Symlink { artifact, subpath } => {
// 						let artifact = artifact
// 							.as_ref()
// 							.map(|edge| try_convert_to_graph_edge(&graph_indices, &edge))
// 							.transpose()?;
// 						let subpath = subpath.clone();
// 						let data = tg::graph::data::Symlink { artifact, subpath };
// 						tg::graph::data::Node::Symlink(data)
// 					},
// 				};
// 				nodes.push(node);
// 			}

// 			// Store the graph.
// 			let graph = tg::graph::Data { nodes };
// 			let bytes = graph.serialize()?;
// 			let id = tg::graph::Id::new(&bytes);
// 			let arg = tg::object::put::Arg { bytes };
// 			self.put_object(&id.clone().into(), arg).await?;

// 			// Create the data for each node.
// 			for index in scc.iter().copied() {
// 				let node = graph_indices.get(&index).copied().unwrap();
// 				let node_data = match &lockfile.nodes[index] {
// 					tg::lockfile::Node::Directory { .. } => {
// 						let data = tg::directory::Data::Graph {
// 							graph: id.clone(),
// 							node,
// 						};
// 						let id = tg::directory::Id::new(&data.serialize()?).into();
// 						let data = tg::artifact::Data::Directory(data);
// 						LockfileNodeData { id, data }
// 					},
// 					tg::lockfile::Node::File { .. } => {
// 						let data = tg::file::Data::Graph {
// 							graph: id.clone(),
// 							node,
// 						};
// 						let id = tg::file::Id::new(&data.serialize()?).into();
// 						let data = tg::artifact::Data::File(data);
// 						LockfileNodeData { id, data }
// 					},
// 					tg::lockfile::Node::Symlink { .. } => {
// 						let data = tg::symlink::Data::Graph {
// 							graph: id.clone(),
// 							node,
// 						};
// 						let id = tg::symlink::Id::new(&data.serialize()?).into();
// 						let data = tg::artifact::Data::Symlink(data);
// 						LockfileNodeData { id, data }
// 					},
// 				};
// 				data[index].replace(node_data);
// 			}
// 		}
// 		let data = data.into_iter().filter_map(|node| node).collect();
// 		Ok(data)
// 	}
// }

// fn try_convert_to_graph_edge(
// 	graph_indices: &BTreeMap<usize, usize>,
// 	edge: &Either<usize, tg::object::Id>,
// ) -> tg::Result<Either<usize, tg::artifact::Id>> {
// 	match edge {
// 		Either::Left(index) => {
// 			let index = graph_indices
// 				.get(index)
// 				.copied()
// 				.ok_or_else(|| tg::error!("incomplete graph"))?;
// 			Ok(Either::Left(index))
// 		},
// 		Either::Right(tg::object::Id::Directory(id)) => Ok(Either::Right(id.clone().into())),
// 		Either::Right(tg::object::Id::File(id)) => Ok(Either::Right(id.clone().into())),
// 		Either::Right(tg::object::Id::Symlink(id)) => Ok(Either::Right(id.clone().into())),
// 		_ => Err(tg::error!("invalid lockfile")),
// 	}
// }

impl Graph {
	#[allow(dead_code)]
	pub(super) async fn print(&self) {
		let mut visited = BTreeSet::new();
		let mut stack: Vec<(usize, usize, Option<PathBuf>)> = vec![(0, 0, None)];
		while let Some((node, depth, subpath)) = stack.pop() {
			for _ in 0..depth {
				eprint!("  ");
			}
			let path = self.nodes[node].arg.path.clone();
			let subpath = subpath.map_or(String::new(), |p| p.display().to_string());
			eprintln!("* {} {}", path.display(), subpath);
			if visited.contains(&node) {
				continue;
			}
			visited.insert(node);
			for edge in &self.nodes[node].edges {
				if let Some(node) = edge.node {
					stack.push((node, depth + 1, edge.subpath.clone()));
				}
			}
		}
	}

	pub(super) async fn validate(&self) -> tg::Result<()> {
		let mut paths = BTreeMap::new();
		let mut stack = vec![0];
		while let Some(node) = stack.pop() {
			let path = self.nodes[node].arg.path.clone();
			if paths.contains_key(&path) {
				continue;
			}
			paths.insert(path, node);
			for edge in &self.nodes[node].edges {
				if let Some(child) = edge.node {
					stack.push(child);
				}
			}
		}
		for (path, node) in &paths {
			let Some(parent) = self.nodes[*node].parent else {
				continue;
			};
			let parent = self.nodes[parent].arg.path.clone();
			let _parent = paths.get(&parent).ok_or_else(
				|| tg::error!(%path = path.display(), %parent = parent.display(), "missing parent node"),
			)?;
		}
		Ok(())
	}
}

async fn root_node_with_subpath(graph: &Graph, node: usize) -> (usize, Option<PathBuf>) {
	// Find the root node.
	let root = get_root_node(graph, node).await;

	// If this is a root node, return it.
	if root == node {
		return (node, None);
	}

	// Otherwise compute the subpath within the root.
	let referent_path = graph.nodes[node].arg.path.clone();
	let root_path = graph.nodes[root].arg.path.clone();
	let subpath = referent_path.strip_prefix(&root_path).unwrap().to_owned();

	// Return the root node and subpath.
	(root, Some(subpath))
}

impl State {
	// Add a new node as a root to the state, and then return the paths of any nodes
	async fn add_root_and_return_dangling_directories(&mut self, node: usize) -> Vec<PathBuf> {
		let new_path = self.graph.nodes[node].arg.path.clone();

		// Update any nodes of the graph that are children.
		let mut roots = Vec::with_capacity(self.roots.len() + 1);
		let mut dangling = Vec::new();
		for root in &self.roots {
			let old_path = self.graph.nodes[*root].arg.path.clone();
			if let Ok(child) = new_path.strip_prefix(&old_path) {
				let dangling_directory = new_path.join(child.ancestors().last().unwrap());
				dangling.push(dangling_directory);
			} else {
				roots.push(*root);
			}
		}

		// Add the new node to the new list of roots.
		if roots.is_empty() {
			roots.push(node);
		}

		// Update the list of roots.
		self.roots = roots;

		// Return any dangling directories.
		dangling
	}
}

async fn get_root_node(graph: &Graph, mut node: usize) -> usize {
	loop {
		let Some(parent) = graph.nodes[node].parent else {
			return node;
		};
		node = parent;
	}
}

impl std::fmt::Display for Edge {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "(reference: {}", self.reference)?;
		if let Some(node) = &self.node {
			write!(f, ", node: {node}")?;
		}
		if let Some(object) = &self.object {
			write!(f, ", object: {object}")?;
		}
		if let Some(path) = &self.path {
			write!(f, ", path: {}", path.display())?;
		}
		if let Some(subpath) = &self.subpath {
			write!(f, ", subpath: {}", subpath.display())?;
		}
		if let Some(tag) = &self.tag {
			write!(f, ", tag: {tag}")?;
		}
		write!(f, ")")?;
		Ok(())
	}
}
