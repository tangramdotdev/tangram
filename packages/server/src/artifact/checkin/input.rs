use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use std::{
	collections::{BTreeMap, BTreeSet},
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
	pub edges: Vec<Edge>,
	pub lockfile: Option<(Arc<tg::Lockfile>, usize)>,
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

struct State {
	ignore: Ignore,
	roots: Vec<usize>,
	graph: Graph,
	visited: BTreeMap<PathBuf, usize>,
}

impl Server {
	pub(super) async fn create_input_graph(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Graph> {
		// Make a best guess for the input.
		let path = self.find_root(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path.display(), "failed to find root path for checkin"),
		)?;

		// Create a graph.
		let mut graph = 'a: {
			let arg_ = tg::artifact::checkin::Arg {
				path,
				..arg.clone()
			};
			let graph = self.collect_input(arg_, progress).await?;

			// Check if the graph contains the path.
			if graph.contains_path(&arg.path).await {
				break 'a graph;
			}

			// Otherwise use the parent.
			let path = arg
				.path
				.parent()
				.ok_or_else(|| tg::error!("cannot check in root"))?
				.to_owned();
			let arg_ = tg::artifact::checkin::Arg { path, ..arg };
			self.collect_input(arg_, progress).await?
		};

		// Validate the input graph.
		graph.validate().await?;

		// Pick lockfiles for each node.
		self.select_lockfiles(&mut graph).await?;

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
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let path = path.parent().unwrap();
		for path in path.ancestors() {
			for root_module_name in tg::package::ROOT_MODULE_FILE_NAMES {
				let root_module_path = path.join(root_module_name);
				if tokio::fs::try_exists(&root_module_path).await.map_err(|source| tg::error!(!source, %root_module_path = root_module_path.display(), "failed to check if root module exists"))? {
					return Ok(path.to_owned());
				}
			}
		}

		// Otherwise return the parent.
		Ok(path.to_owned())
	}

	async fn collect_input(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Graph> {
		// Create the ignore tree.
		let ignore = self.ignore_for_checkin().await?;

		// Initialize state.
		let state = RwLock::new(State {
			ignore,
			roots: Vec::new(),
			visited: BTreeMap::new(),
			graph: Graph { nodes: Vec::new() },
		});

		// Collect input.
		self.collect_input_inner(None, arg.path.as_ref(), &arg, &state, progress)
			.await?;

		let State { graph, .. } = state.into_inner();
		Ok(graph)
	}

	async fn collect_input_inner(
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
			|source| tg::error!(!source, %path = path.display(), "failed to canonicalize path"),
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

		// Get a write lock on the state to avoid a race condition.
		let mut state_ = state.write().await;

		// Check if this path has already been visited and return it.
		if let Some(node) = state_.visited.get(&absolute_path) {
			return Ok(*node);
		}

		// Lookup the parent if it exists.
		let parent = absolute_path
			.parent()
			.and_then(|parent| state_.visited.get(parent).copied());

		// Create a new node.
		let arg_ = tg::artifact::checkin::Arg {
			path: absolute_path.clone(),
			..arg.clone()
		};
		let node = Node {
			arg: arg_,
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
			Box::pin(self.collect_input_inner(None, &dangling, arg, state, progress))
				.await
				.map_err(
					|source| tg::error!(!source, %path = dangling.display(), "failed to collect dangling directory"),
				)?;
		}

		// If this is a module file, ensure the parent is collected.
		if metadata.is_file() && tg::package::is_module_path(&absolute_path) {
			let parent = absolute_path.parent().unwrap().to_owned();
			let parent = Box::pin(self.collect_input_inner(None, &parent, arg, state, progress))
				.await
				.map_err(
					|source| tg::error!(!source, %path = absolute_path.display(), "failed to collect input of parent"),
				)?;
			state.write().await.graph.nodes[node].parent.replace(parent);
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
					.map_err(|source| tg::error!(!source, "failed to get file type"))?;
				if state
					.write()
					.await
					.ignore
					.should_ignore(&path.join(&name), file_type)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to check if the path should be ignored")
					})? {
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
					let node = Box::pin(self.collect_input_inner(
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
					let path = crate::util::path::diff(&arg.path, &path)?;
					let edge = Edge {
						reference,
						subpath: None,
						node: Some(node),
						object: None,
						path: Some(path),
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
		if let Some(data) = xattr::get(path, tg::file::XATTR_DATA_NAME)
			.map_err(|source| tg::error!(!source, "failed to read file xattr"))?
		{
			let xattr = serde_json::from_slice(&data)
				.map_err(|source| tg::error!(!source, "failed to deserialize xattr"))?;

			let dependencies = match xattr {
				tg::file::Data::Graph { graph, node } => {
					let graph_ = tg::Graph::with_id(graph.clone()).object(self).await?;
					graph_.nodes[node]
						.try_unwrap_file_ref()
						.map_err(|_| tg::error!("expected a file"))?
						.dependencies
						.iter()
						.map(|(reference, referent)| {
							let graph_ = graph_.clone();
							let graph = graph.clone();
							async move {
								let item = match &referent.item {
									Either::Left(node) => match &graph_.nodes[*node] {
										tg::graph::object::Node::Directory(_) => {
											let data =
												tg::directory::Data::Graph { graph, node: *node };
											let bytes = data.serialize()?;
											let id = tg::directory::Id::new(&bytes);
											id.into()
										},
										tg::graph::object::Node::File(_) => {
											let data = tg::file::Data::Graph { graph, node: *node };
											let bytes = data.serialize()?;
											let id = tg::file::Id::new(&bytes);
											id.into()
										},
										tg::graph::object::Node::Symlink(_) => {
											let data =
												tg::symlink::Data::Graph { graph, node: *node };
											let bytes = data.serialize()?;
											let id = tg::symlink::Id::new(&bytes);
											id.into()
										},
									},
									Either::Right(object) => object.id(self).await?,
								};
								let dependency = tg::Referent {
									item,
									path: referent.path.clone(),
									tag: referent.tag.clone(),
									subpath: referent.subpath.clone(),
								};
								Ok::<_, tg::Error>((reference.clone(), dependency))
							}
						})
						.collect::<FuturesUnordered<_>>()
						.try_collect()
						.await?
				},
				tg::file::Data::Normal { dependencies, .. } => dependencies,
			};
			let edges = dependencies
				.into_iter()
				.map(|(reference, referent)| Edge {
					reference,
					node: None,
					object: Some(referent.item),
					subpath: referent.subpath,
					path: referent.path,
					tag: referent.tag,
				})
				.collect();
			return Ok(edges);
		}

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
		let analysis = crate::compiler::Compiler::analyze_module(text).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to analyze module"),
		)?;

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

						let absolute_path = crate::util::fs::canonicalize_parent(path.parent().unwrap().join(import_path)).await.map_err(|source| tg::error!(!source, "failed to canonicalize the path's parent"))?;
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
							self.collect_input_inner(Some(referrer), &absolute_path, &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						} else {
							// Otherwise, treat it as a relative path.

							// Get the parent of the referrer.
							let parent = state.read().await.graph.nodes[referrer].parent.ok_or_else(|| tg::error!("expected a parent"))?;

							// Recurse.
							self.collect_input_inner(Some(parent), import_path.as_ref(), &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						};


						// If the child is a module and the import kind is not directory, get the subpath.
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
							root_node_with_subpath(&state.read().await.graph, child).await
						};

						// Create the edge.
						let path = crate::util::path::diff( &arg.path, &child_path)?;
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
		let target_absolute_path =
			crate::util::fs::canonicalize_parent(path.parent().unwrap().join(&target))
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to canonicalize the path's parent")
				})?;

		// Check if this is a checkin of a bundled artifact.
		let path = crate::util::path::diff(&arg.path, &target_absolute_path)?;
		let edge = if let Ok(subpath) = target_absolute_path.strip_prefix(self.artifacts_path()) {
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

	pub(super) async fn select_lockfiles(&self, graph: &mut Graph) -> tg::Result<()> {
		let visited = RwLock::new(BTreeSet::new());
		self.select_lockfiles_inner(graph, 0, &visited).await?;
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
			crate::lockfile::try_get_lockfile_node_for_module_path(path.as_ref())
				.await
				.map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to get lockfile for path"),
				)?
				.map(|(lockfile, node)| Ok::<_, tg::Error>((Arc::new(lockfile), node)))
				.transpose()?
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

impl Graph {
	async fn contains_path(&self, path: &Path) -> bool {
		for node in &self.nodes {
			if node.arg.path == path {
				return true;
			}
		}
		false
	}

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
