use crate::{util::path::Ext as _, Server};
use futures::{future, stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use std::{
	collections::{BTreeMap, BTreeSet},
	path::{Path, PathBuf},
	sync::{Arc, Weak},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_ignore::Ignore;
use tokio::sync::RwLock;

// List of ignore files that checkin will consider.
const IGNORE_FILES: [&str; 3] = [".tangramignore", ".tgignore", ".gitignore"];

// List of patterns that checkin will ignore.
const DENY: [&str; 2] = [".DS_STORE", ".git"];

// List of patterns that checkin will not ignore.
const ALLOW: [&str; 0] = [];

#[derive(Clone, Debug)]
pub struct Graph {
	pub arg: tg::artifact::checkin::Arg,
	pub edges: Vec<Edge>,
	pub is_direct_dependency: bool,
	pub metadata: std::fs::Metadata,
	pub lockfile: Option<(Arc<tg::Lockfile>, usize)>,
	pub parent: Option<Weak<RwLock<Self>>>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub graph: Option<Node>,
	pub object: Option<tg::object::Id>,
	pub tag: Option<tg::Tag>,
}

pub type Node = Either<Arc<RwLock<Graph>>, Weak<RwLock<Graph>>>;

struct State {
	ignore: Ignore,
	roots: Vec<Arc<RwLock<Graph>>>,
	visited: BTreeMap<PathBuf, Arc<RwLock<Graph>>>,
}

impl Server {
	pub(super) async fn create_input_graph(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		// Make a best guess for the input.
		let path = self.find_root(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path.display(), "failed to find root path for checkin"),
		)?;

		// Create a graph.
		let input = 'a: {
			let arg_ = tg::artifact::checkin::Arg {
				path,
				..arg.clone()
			};
			let graph = self.collect_input(arg_, progress).await?;

			// Check if the graph contains the path.
			if graph.read().await.contains_path(&arg.path).await {
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
		Graph::validate(input.clone()).await?;
		Ok(input)
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
	) -> tg::Result<Arc<RwLock<Graph>>> {
		// Create the ignore tree.
		let ignore = Ignore::new(IGNORE_FILES, ALLOW, DENY)
			.await
			.map_err(|source| tg::error!(!source, "failed to create ignore tree"))?;

		// Initialize state.
		let state = RwLock::new(State {
			ignore,
			roots: Vec::new(),
			visited: BTreeMap::new(),
		});

		// Collect input.
		let input = self.collect_input_inner(None, arg.path.as_ref(), &arg, &state, progress);

		Ok(input.await?.unwrap_left())
	}

	async fn collect_input_inner(
		&self,
		referrer: Option<Node>,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Node> {
		// Get the full path.
		let path = if path.is_absolute() {
			// If the absolute path is provided, use it.
			path.to_owned()
		} else {
			let referrer = match referrer.ok_or_else(|| tg::error!("expected a referrer"))? {
				Either::Left(strong) => strong,
				Either::Right(weak) => weak.upgrade().unwrap(),
			};
			let referrer_path = referrer.read().await.arg.path.clone();
			referrer_path.join(path.strip_prefix("./").unwrap_or(path))
		};
		let parent = path.parent().unwrap();
		let file_name = path.file_name().unwrap();

		// Canonicalize the parent and join with the file name.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let absolute_path = tokio::fs::canonicalize(parent)
			.await
			.map_err(
				|source| tg::error!(!source, %path = parent.display(), "failed to canonicalize the path"),
			)?
			.join(file_name);
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
			return Ok(Either::Left(node.clone()));
		}

		// Lookup the parent if it exists.
		let parent = absolute_path
			.parent()
			.and_then(|parent| state_.visited.get(parent).map(Arc::downgrade));

		// Create a new node.
		let arg_ = tg::artifact::checkin::Arg {
			path: absolute_path.clone(),
			..arg.clone()
		};
		let node = Arc::new(RwLock::new(Graph {
			arg: arg_,
			edges: Vec::new(),
			is_direct_dependency: false,
			metadata: metadata.clone(),
			lockfile: None,
			parent: parent.clone(),
		}));
		state_.visited.insert(absolute_path.clone(), node.clone());

		// Update the roots if necessary. This may result in dangling directories that also need to be collected.
		let dangling = if parent.is_none() {
			state_
				.add_root_and_return_dangling_directories(node.clone())
				.await
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
			let parent = match parent {
				Either::Left(strong) => Arc::downgrade(&strong),
				Either::Right(weak) => weak,
			};
			node.write().await.parent.replace(parent);
		}

		// Get the edges.
		let edges = self
			.get_edges(node.clone(), &absolute_path, arg, state, progress)
			.await
			.map_err(
				|source| tg::error!(!source, %path = absolute_path.display(), "failed to get edges"),
			)?;

		// Update the node.
		node.write().await.edges = edges;

		// Return the created node.
		Ok(Either::Left(node))
	}

	async fn get_edges(
		&self,
		referrer: Arc<RwLock<Graph>>,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		let metadata = referrer.read().await.metadata.clone();
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
		referrer: Arc<RwLock<Graph>>,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		// Get the directory entries.
		let mut names = Vec::new();
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let mut entries = tokio::fs::read_dir(&path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the directory entries"),
		)?;
		loop {
			let Some(entry) = entries.next_entry().await.map_err(
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
		drop(entries);
		drop(permit);

		let vec: Vec<_> = names
			.into_iter()
			.map(|name| {
				let referrer = referrer.clone();
				async move {
					let path = path.join(&name);

					// Follow the edge.
					let graph = Box::pin(self.collect_input_inner(
						Some(Either::Right(Arc::downgrade(&referrer))),
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
						graph: Some(graph),
						object: None,
						tag: None,
					};
					Ok::<_, tg::Error>(edge)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(vec)
	}

	async fn get_file_edges(
		&self,
		referrer: Arc<RwLock<Graph>>,
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
				tg::file::Data::Normal { dependencies, .. } => dependencies,
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
										tg::graph::node::Node::Directory(_) => {
											tg::directory::Data::Graph { graph, node: *node }
												.id()?
												.into()
										},
										tg::graph::node::Node::File(_) => {
											tg::file::Data::Graph { graph, node: *node }
												.id()?
												.into()
										},
										tg::graph::node::Node::Symlink(_) => {
											tg::symlink::Data::Graph { graph, node: *node }
												.id()?
												.into()
										},
									},
									Either::Right(object) => object.id(self).await?,
								};
								let dependency = tg::Referent {
									item,
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
			};
			let edges = dependencies
				.into_iter()
				.map(|(reference, referent)| {
					let arg = arg.clone();
					let referrer = referrer.clone();
					async move {
						let root_path = get_root_node(Either::Left(referrer.clone()))
							.await
							.read()
							.await
							.arg
							.path
							.clone();
						let id = referent.item;
						let path = reference
							.item()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.options()?.path.as_ref());

						// Don't follow paths that point outside the root.
						let referrer_path = referrer.read().await.arg.path.clone();
						let is_external_path = path
							.as_ref()
							.map(|path| referrer_path.parent().unwrap().join(path))
							.and_then(|path| path.diff(&root_path))
							.map_or(false, |diff| {
								diff.components().next().is_some_and(|component| {
									matches!(component, std::path::Component::ParentDir)
								})
							});

						// Recurse if necessary.
						let (graph, subpath) = match path {
							Some(path) if !is_external_path => {
								// Get the parent of the referrer.
								let parent = referrer
									.read()
									.await
									.parent
									.as_ref()
									.ok_or_else(|| tg::error!("expected a parent"))?
									.clone();

								// Collect the input of the referrent.
								let graph = self
									.collect_input_inner(
										Some(Either::Right(parent)),
										path,
										&arg,
										state,
										progress,
									)
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to collect child input")
									})?;
								let (graph, subpath) =
									root_node_with_subpath(referrer.clone(), graph).await;
								(Some(graph), subpath)
							},
							_ => (None, None),
						};

						let edge = Edge {
							reference,
							graph,
							object: Some(id),
							subpath,
							tag: referent.tag,
						};

						Ok::<_, tg::Error>(edge)
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;

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
		referrer: Arc<RwLock<Graph>>,
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
				let referrer = referrer.clone();

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
						let root_path = get_root_node(Either::Left(referrer.clone()))
							.await
							.read()
							.await
							.arg
							.path
							.clone();

						let absolute_path = path.parent().unwrap().join(import_path.strip_prefix("./").unwrap_or(import_path));
						let is_external = absolute_path.diff(&root_path).map_or(false, |path| path.components().next().is_some_and(|component| matches!(component, std::path::Component::ParentDir)));

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
							self.collect_input_inner(Some(Either::Left(referrer.clone())), &absolute_path, &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						} else {
							// Otherwise, treat it as a relative path.

							// Get the parent of the referrer.
							let parent = referrer.read().await.parent.as_ref().ok_or_else(|| tg::error!("expected a parent"))?.clone();

							// Recurse.
							self.collect_input_inner(Some(Either::Right(parent)), import_path.as_ref(), &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						};

						let (graph, subpath) = root_node_with_subpath(referrer.clone(), child.clone()).await;

						// Create the edge.
						let edge = Edge {
							reference,
							graph: Some(graph),
							object: None,
							subpath,
							tag: None,
						};

						return Ok(edge);
					}

					Ok(Edge {
						reference: import.reference,
						graph: None,
						object: None,
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
		referrer: Arc<RwLock<Graph>>,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
		if !referrer.read().await.edges.is_empty() {
			return Ok(referrer.read().await.edges.clone());
		}

		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let target = tokio::fs::read_link(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to read symlink"),
		)?;
		drop(permit);

		if target.is_absolute() {
			return Ok(Vec::new());
		}

		let parent = referrer.read().await.parent.clone().map(Either::Right);

		// Get the absolute path of the target.
		let target_absolute_path = path.parent().unwrap().join(&target);
		let target_absolute_path = tokio::fs::canonicalize(&target_absolute_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
			.join(target_absolute_path.file_name().unwrap());

		// Collect the input of the target.
		let child = if state
			.read()
			.await
			.find_root_of_path(&target_absolute_path)
			.await
			.is_none()
		{
			Box::pin(self.collect_input_inner(
				Some(Either::Left(referrer.clone())),
				&target_absolute_path,
				arg,
				state,
				progress,
			))
			.await?
		} else {
			Box::pin(self.collect_input_inner(parent, &target, arg, state, progress)).await?
		};

		// Get the root node and subpath.
		let (graph, subpath) = root_node_with_subpath(referrer.clone(), child).await;

		Ok(vec![Edge {
			reference: tg::Reference::with_path(target),
			graph: Some(graph),
			object: None,
			subpath,
			tag: None,
		}])
	}
}

impl Server {
	pub(super) async fn select_lockfiles(&self, input: Arc<RwLock<Graph>>) -> tg::Result<()> {
		let visited = RwLock::new(BTreeSet::new());
		self.select_lockfiles_inner(input, &visited).await?;
		Ok(())
	}

	async fn select_lockfiles_inner(
		&self,
		input: Arc<RwLock<Graph>>,
		visited: &RwLock<BTreeSet<PathBuf>>,
	) -> tg::Result<()> {
		// Check if this path is visited or not.
		let path = input.read().await.arg.path.clone();
		if visited.read().await.contains(&path) {
			return Ok(());
		}

		// Mark this node as visited.
		visited.write().await.insert(path.clone());

		// If this is not a root module, inherit the lockfile of the referrer.
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
			let parent = input
				.read()
				.await
				.parent
				.as_ref()
				.map(|parent| parent.upgrade().unwrap());
			if let Some(parent) = parent {
				parent.read().await.lockfile.clone()
			} else {
				None
			}
		};
		input.write().await.lockfile = lockfile;

		// Recurse.
		let edges = input.read().await.edges.clone();
		edges
			.into_iter()
			.filter_map(|edge| {
				let child = edge.node()?;
				let fut = Box::pin(self.select_lockfiles_inner(child, visited));
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		Ok(())
	}
}

impl Graph {
	async fn contains_path(&self, path: &Path) -> bool {
		let visited = std::sync::RwLock::new(BTreeSet::new());
		self.contains_path_inner(path, &visited).await
	}

	async fn contains_path_inner<'a>(
		&self,
		path: &'a Path,
		visited: &std::sync::RwLock<BTreeSet<&'a Path>>,
	) -> bool {
		if visited.read().unwrap().contains(&path) {
			return false;
		}
		if self.arg.path == path {
			return true;
		}
		visited.write().unwrap().insert(path);
		self.edges
			.iter()
			.filter_map(Edge::node)
			.map(|child| async move {
				let child = child.read().await;
				Box::pin(child.contains_path_inner(path, visited)).await
			})
			.collect::<FuturesUnordered<_>>()
			.any(future::ready)
			.await
	}
}

impl Edge {
	pub fn node(&self) -> Option<Arc<RwLock<Graph>>> {
		self.graph.as_ref().map(|node| match node {
			Either::Left(node) => node.clone(),
			Either::Right(node) => node.upgrade().unwrap(),
		})
	}
}

impl Graph {
	#[allow(dead_code)]
	pub(super) async fn print(self_: Arc<RwLock<Graph>>) {
		let mut visited = BTreeSet::new();
		let mut stack: Vec<(Node, usize, Option<PathBuf>)> = vec![(Either::Left(self_), 0, None)];
		while let Some((node, depth, subpath)) = stack.pop() {
			for _ in 0..depth {
				eprint!("  ");
			}
			match node {
				Either::Left(strong) => {
					let path = strong.read().await.arg.path.clone();
					if let Some(subpath) = subpath {
						eprintln!(
							"* strong {path:#?} {} {:x?}",
							subpath.display(),
							Arc::as_ptr(&strong)
						);
					} else {
						eprintln!("* strong {path:#?} {:x?}", Arc::as_ptr(&strong));
					}
					if !visited.contains(&path) {
						visited.insert(path);
						stack.extend(strong.read().await.edges.iter().filter_map(|edge| {
							Some((edge.graph.clone()?, depth + 1, edge.subpath.clone()))
						}));
					}
				},
				Either::Right(weak) => {
					let path = weak.upgrade().unwrap().read().await.arg.path.clone();
					if let Some(subpath) = subpath {
						eprintln!(
							"* weak {path:#?} {} {:x?}",
							subpath.display(),
							Weak::as_ptr(&weak)
						);
					} else {
						eprintln!("* weak {path:#?} {:x?}", Weak::as_ptr(&weak));
					}
				},
			}
		}
	}

	pub(super) async fn validate(this: Arc<RwLock<Graph>>) -> tg::Result<()> {
		let mut paths = BTreeMap::new();
		let mut stack = vec![this];
		while let Some(node) = stack.pop() {
			let path = node.read().await.arg.path.clone();
			if paths.contains_key(&path) {
				continue;
			}
			paths.insert(path, node.clone());
			for edge in &node.read().await.edges {
				if let Some(child) = &edge.graph {
					let child = match child {
						Either::Left(child) => child.clone(),
						Either::Right(child) => child.upgrade().unwrap(),
					};
					stack.push(child);
				}
			}
		}

		for (path, node) in &paths {
			let Some(parent) = node.read().await.parent.clone() else {
				continue;
			};
			let parent = parent
				.upgrade()
				.ok_or_else(
					|| tg::error!(%node = path.display(), "internal error, parent went out of scope"),
				)?
				.read()
				.await
				.arg
				.path
				.clone();
			let root = get_root_node(Either::Left(node.clone()))
				.await
				.read()
				.await
				.arg
				.path
				.clone();
			let _parent = paths.get(&parent)
				.ok_or_else(|| tg::error!(%root = root.display(), %path = path.display(), %parent = parent.display(), "missing parent node"))?;
		}

		Ok(())
	}
}

impl Edge {
	#[allow(dead_code)]
	pub(super) async fn print(&self) {
		eprint!("({}", self.reference);
		if let Some(node) = self.node() {
			eprint!(" path: {}", node.read().await.arg.path.display());
		}
		if let Some(subpath) = &self.subpath {
			eprint!(" subpath: {}", subpath.display());
		}
		eprintln!(")");
	}
}

async fn root_node_with_subpath(
	referrer: Arc<RwLock<Graph>>,
	child: Node,
) -> (Node, Option<PathBuf>) {
	let root = get_root_node(child.clone()).await;
	let strong = match child.clone() {
		Either::Left(strong) => strong,
		Either::Right(weak) => weak.upgrade().unwrap(),
	};

	// If this is a root node, return it.
	if strong.read().await.parent.is_none() {
		return (child, None);
	}

	// Otherwise compute the subpath within the root.
	let path = strong.read().await.arg.path.clone();
	let root_path = root.read().await.arg.path.clone();
	let subpath = path.diff(&root_path).unwrap();

	// If the root is shared with the referrer, return a weak reference. Otherwise return a strong reference.
	let referrer_root = get_root_node(Either::Left(referrer).clone()).await;
	let root = if Arc::ptr_eq(&referrer_root, &root) {
		Either::Right(Arc::downgrade(&root))
	} else {
		Either::Left(root)
	};

	(root, Some(subpath))
}

impl State {
	// Add a new node as a root to the state, and then return the paths of any nodes
	async fn add_root_and_return_dangling_directories(
		&mut self,
		node: Arc<RwLock<Graph>>,
	) -> Vec<PathBuf> {
		let new_path = node.read().await.arg.path.clone();

		// Update any nodes of the graph that are children.
		let mut roots = Vec::with_capacity(self.roots.len() + 1);
		let mut dangling = Vec::new();
		for root in &self.roots {
			let old_path = root.read().await.arg.path.clone();
			let diff = new_path.diff(&old_path).unwrap();
			if let Ok(child) = diff.strip_prefix("./") {
				let dangling_directory = new_path.join(child.ancestors().last().unwrap());
				dangling.push(dangling_directory);
			} else {
				roots.push(root.clone());
			}
		}
		// Add the new node to the new list of roots.
		roots.push(node);

		// Update the list of roots.
		self.roots = roots;

		// Return any dangling directories.
		dangling
	}

	async fn find_root_of_path(&self, path: &Path) -> Option<Arc<RwLock<Graph>>> {
		for root in &self.roots {
			if path
				.diff(&root.read().await.arg.path)
				.map_or(false, |path| {
					path.components().next().is_some_and(|component| {
						!matches!(component, std::path::Component::ParentDir)
					})
				}) {
				return Some(root.clone());
			}
		}
		None
	}
}

async fn get_root_node(mut node: Node) -> Arc<RwLock<Graph>> {
	loop {
		let strong = match node {
			Either::Left(strong) => strong,
			Either::Right(weak) => weak.upgrade().unwrap(),
		};
		let Some(parent) = strong.read().await.parent.clone() else {
			return strong;
		};
		node = Either::Right(parent);
	}
}
