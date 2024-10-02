use crate::Server;
use futures::{
	future,
	stream::{self, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::{Path, PathBuf},
	sync::{Arc, Weak},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_ignore::Ignore;
use tg::path::Ext as _;
use tokio::sync::RwLock;

const IGNORE_FILES: [&str; 2] = [".tgignore", ".gitignore"];

#[derive(Clone, Debug)]
pub struct Graph {
	pub arg: tg::artifact::checkin::Arg,
	pub edges: Vec<Edge>,
	pub root: Option<PathBuf>,
	pub is_direct_dependency: bool,
	pub metadata: std::fs::Metadata,
	pub lockfile: Option<(Arc<tg::Lockfile>, usize)>,
	pub parent: Option<Weak<RwLock<Self>>>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub graph: Option<Node>,
	pub object: Option<tg::object::Id>,
	pub tag: Option<tg::Tag>,
}

pub type Node = Either<Arc<RwLock<Graph>>, Weak<RwLock<Graph>>>;

struct State {
	ignore: Ignore,
	roots: BTreeMap<PathBuf, Arc<RwLock<Graph>>>,
	visited: BTreeMap<PathBuf, Weak<RwLock<Graph>>>,
}

impl Server {
	pub(super) async fn create_input_graph(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: &super::ProgressState,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		// Make a best guess for the input.
		let path = self.find_root(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path.display(), "failed to find root path for checkin"),
		)?;

		// Create a graph.
		let arg_ = tg::artifact::checkin::Arg {
			path,
			..arg.clone()
		};
		let graph = self.collect_input(arg_, progress).await?;

		// Check if the graph contains the path.
		if graph.read().await.contains_path(&arg.path).await {
			return Ok(graph);
		}

		// Otherwise use the parent.
		let path = arg
			.path
			.parent()
			.ok_or_else(|| tg::error!("cannot check in root"))?
			.to_owned();
		let arg_ = tg::artifact::checkin::Arg { path, ..arg };
		self.collect_input(arg_, progress).await
	}

	async fn find_root(&self, path: &Path) -> tg::Result<PathBuf> {
		if !path.is_absolute() {
			return Err(tg::error!(%path = path.display(), "expected an absolute path"));
		}

		if !tg::package::is_module_path(path) {
			return Ok(path.to_owned());
		}

		// Look for a `tangram.ts`.
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
		progress: &super::ProgressState,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		let ignore = Ignore::new(IGNORE_FILES)
			.await
			.map_err(|source| tg::error!(!source, "failed to create ignore tree"))?;

		let state = RwLock::new(State {
			ignore,
			roots: BTreeMap::new(),
			visited: BTreeMap::new(),
		});

		let input = self
			.collect_input_inner(None, arg.path.as_ref(), &arg, &state, progress)
			.await?
			.unwrap_left();

		Ok(input)
	}
	async fn collect_input_inner(
		&self,
		referrer: Option<Node>,
		path: &std::path::Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Node> {
		let absolute_path = match &referrer {
			_ if path.is_absolute() => path.to_owned(),
			Some(Either::Left(strong)) => strong.read().await.arg.path.join(path),
			Some(Either::Right(weak)) => weak.upgrade().unwrap().read().await.arg.path.join(path),
			None => return Err(tg::error!("expeccted a referrer")),
		};
		let absolute_path = tokio::fs::canonicalize(&absolute_path).await.map_err(
			|source| tg::error!(!source, %path = absolute_path.display(), "failed to canonicalize the path"),
		)?;

		let node = if path.is_absolute() {
			// If this is an absolute path, create a new node.
			self.get_or_create_input_node(referrer, &absolute_path, arg, state)
				.await
				.map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to create input node"),
				)?
		} else {
			// Otherwise, create a node for all the paths along the way.
			let mut node = referrer
				.ok_or_else(|| tg::error!(%path = path.display(), "expected a referrer"))?;

			// Walk the components of the path.
			let mut components = path.components().peekable();
			while let Some(component) = components.next() {
				let is_end = components.peek().is_none();
				node = match component {
					// Ignore "." components.
					std::path::Component::CurDir => continue,

					// Try and get the parent of the current node.
					std::path::Component::ParentDir => {
						let node = match &node {
							Either::Left(strong) => strong.clone(),
							Either::Right(weak) => weak.upgrade().unwrap(),
						};

						if !node.read().await.metadata.is_dir() {
							return Err(
								tg::error!(%path = node.read().await.arg.path.display(), "expected a directory"),
							);
						}

						let parent = node.read().await.parent.clone().ok_or_else(
							|| tg::error!(%path = path.display(), "expected a parent node"),
						)?;

						Either::Right(parent)
					},

					// If this is the last component, recurse.
					std::path::Component::Normal(name) if is_end => {
						if name.to_str().is_none() {
							return Err(
								tg::error!(%path = absolute_path.display(), "invalid path"),
							);
						}
						let child = self
							.get_or_create_input_node(
								Some(node.clone()),
								&absolute_path,
								arg,
								state,
							)
							.await?;

						// Add to the parent.
						let parent = match &node {
							Either::Left(strong) => strong.clone(),
							Either::Right(weak) => weak.upgrade().unwrap(),
						};
						let edge = Edge {
							reference: tg::Reference::with_path(name),
							graph: Some(child.clone()),
							object: None,
							tag: None,
						};
						add_edge(parent, edge).await;
						child
					},

					// Otherwise get or create intermediate directories.
					std::path::Component::Normal(name) => {
						let node = match &node {
							Either::Left(strong) => strong.clone(),
							Either::Right(weak) => weak.upgrade().unwrap(),
						};
						let name = name
							.to_str()
							.ok_or_else(|| tg::error!("invalid directory entry name"))?
							.to_owned();
						self.get_or_create_intermediate_input_node(node, name, arg, state)
							.await?
					},

					// Error for root/prefix components.
					_ => {
						return Err(tg::error!(%path = path.display(), "unexpected path component"))
					},
				}
			}

			node
		};

		// If the node is a weak reference, don't recurse.
		let Either::Left(node) = node else {
			return Ok(node);
		};

		// Recurse.
		let edges = self
			.get_edges(node.clone(), absolute_path.as_ref(), arg, state, progress)
			.await?;
		for edge in edges {
			add_edge(node.clone(), edge).await;
		}

		// Send a new progress report.
		progress.report_input_progress();

		// Return the node.
		Ok(Either::Left(node))
	}

	async fn try_resolve_symlink_node(&self, mut node: Node) -> tg::Result<Option<Node>> {
		loop {
			let strong = match &node {
				Either::Left(strong) => strong.clone(),
				Either::Right(weak) => weak.upgrade().unwrap(),
			};
			let strong_ = strong.read().await;
			if strong_.metadata.is_symlink() {
				let Some(next) = strong_.edges.first().and_then(|edge| edge.graph.clone()) else {
					return Ok(None);
				};
				node = next;
			} else {
				return Ok(Some(node));
			}
		}
	}
	async fn get_edges(
		&self,
		referrer: Arc<RwLock<Graph>>,
		path: &std::path::Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Vec<Edge>> {
		let metadata = referrer.read().await.metadata.clone();
		if metadata.is_dir() {
			if let Some(root_module_file_name) =
				tg::package::try_get_root_module_file_name_for_package_path(path).await?
			{
				let graph = Box::pin(self.collect_input_inner(
					Some(Either::Left(referrer.clone())),
					root_module_file_name.as_ref(),
					arg,
					state,
					progress,
				))
				.await
				.map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to collect package input"),
				)?;

				let edge = Edge {
					reference: tg::Reference::with_path(root_module_file_name),
					graph: Some(graph),
					object: None,
					tag: None,
				};
				return Ok(vec![edge]);
			}
			self.get_directory_edges(referrer, path, arg, state, progress)
				.await
		} else if metadata.is_file() {
			self.get_file_edges(referrer, path, arg, state, progress)
				.await
		} else if metadata.is_symlink() {
			let target = tokio::fs::read_link(path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to read symlink"),
			)?;
			let child = Box::pin(self.collect_input_inner(
				Some(Either::Left(referrer)),
				&target,
				arg,
				state,
				progress,
			))
			.await?;
			Ok(vec![Edge {
				reference: tg::Reference::with_path(target),
				graph: Some(child),
				object: None,
				tag: None,
			}])
		} else {
			Err(tg::error!("invalid file type"))
		}
	}

	async fn get_directory_edges(
		&self,
		referrer: Arc<RwLock<Graph>>,
		path: &std::path::Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Vec<Edge>> {
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let entries = tokio::fs::read_dir(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read directory"))?;
		let vec: Vec<_> = stream::try_unfold(entries, |mut entries| async {
			let Some(entry) = entries
				.next_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to get directory entry"))?
			else {
				return Ok(None);
			};

			// Get the file name and path.
			let name = entry
				.file_name()
				.to_str()
				.ok_or_else(|| tg::error!("non-utf8 file name"))?
				.to_owned();
			let path = path.join(&name);

			// Check if we should ignore it.
			let metadata = entry
				.metadata()
				.await
				.map_err(|source| tg::error!(!source, "failed to get metadata"))?;
			if state
				.write()
				.await
				.ignore
				.should_ignore(path.as_ref(), &metadata)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to check if the path should be ignored")
				})? {
				return Ok(Some((None, entries)));
			}

			// Create edge metadata.
			let reference = tg::Reference::with_path(&name);

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
				|source| tg::error!(!source, %path = path.join(name).display(), "failed to collect child input"),
			)?;

			// Create the edge.
			let edge = Edge {
				reference,
				graph: Some(graph),
				object: None,
				tag: None,
			};
			Ok::<_, tg::Error>(Some((Some(edge), entries)))
		})
		.collect::<FuturesUnordered<_>>()
		.await
		.into_iter()
		.try_collect()?;
		Ok(vec.into_iter().flatten().collect())
	}

	async fn get_file_edges(
		&self,
		referrer: Arc<RwLock<Graph>>,
		path: &std::path::Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Vec<Edge>> {
		if let Some(data) = xattr::get(path, tg::file::XATTR_NAME)
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
						.map(|(reference, dependency)| {
							let graph_ = graph_.clone();
							let graph = graph.clone();
							async move {
								let object = match &dependency.object {
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
								let dependency = tg::file::data::Dependency {
									object,
									tag: dependency.tag.clone(),
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
				.map(|(reference, dependency)| {
					let arg = arg.clone();
					let referrer = referrer.clone();
					async move {
						let referrer_ = referrer.read().await;
						let root_path = referrer_
							.root
							.clone()
							.unwrap_or_else(|| referrer_.arg.path.clone());
						let id = dependency.object;
						let path = reference
							.path()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.query()?.path.as_ref());

						// Don't follow paths that point outside the root.
						let is_external_path = path
							.as_ref()
							.map(|path| referrer_.arg.path.parent().unwrap().join(path))
							.and_then(|path| path.diff(&root_path))
							.map_or(false, |path| {
								matches!(
									path.components().next(),
									Some(std::path::Component::ParentDir)
								)
							});

						let graph = if is_external_path {
							None
						} else if let Some(path) = path {
							// Get the parent of the referrer.
							let parent = referrer_
								.parent
								.as_ref()
								.ok_or_else(|| tg::error!("expected a parent"))?
								.clone();

							// Recurse.
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

							Some(graph)
						} else {
							None
						};
						let edge = Edge {
							reference,
							graph,
							object: Some(id),
							tag: dependency.tag,
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
		path: &std::path::Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
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
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| import.reference.query()?.path.as_ref());
					if let Some(import_path) = import_path {
						// Create the reference.
						let reference = import.reference.clone();

						// Check if the import points outside the package.
						let root_path = {
							let referrer = referrer.read().await;
							referrer.root.clone().unwrap_or_else(|| referrer.arg.path.clone())
						};

						let absolute_path = path.parent().unwrap().join(import_path.strip_prefix("./").unwrap_or(import_path));
						let is_external = absolute_path.diff(&root_path).map_or(false, |path| path.is_external());

						// If the import is of a module and points outside the root, return an error.
						if (import_path.is_absolute() ||
							is_external) && tg::package::is_module_path(import_path.as_ref()) {
							return Err(
								tg::error!(%root = root_path.display(), %path = import_path.display(), "cannot import module outside of the package"),
							);
						}

						let graph = if is_external {
							// If this is an external import, treat it as a root using the absolute path.
							self.collect_input_inner(Some(Either::Left(referrer)), &absolute_path, &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						} else {
							// Otherwise, treat it as a relative path.

							// Get the parent of the referrer.
							let parent = referrer.read().await.parent.as_ref().ok_or_else(|| tg::error!("expected a parent"))?.clone();

							// Recurse.
							self.collect_input_inner(Some(Either::Right(parent)), import_path.as_ref(), &arg, state, progress).await.map_err(|source| tg::error!(!source, "failed to collect child input"))?
						};

						// Create the edge.
						let edge = Edge {
							reference,
							graph: Some(graph),
							object: None,
							tag: None,
						};

						return Ok(edge);
					}

					Ok(Edge {
						reference: import.reference,
						graph: None,
						object: None,
						tag: None,
					})
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		Ok(edges)
	}

	async fn get_or_create_input_node(
		&self,
		referrer: Option<Node>,
		path: &std::path::Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
	) -> tg::Result<Node> {
		let mut state = state.write().await;

		// Avoid a subtle race condition by checking if this path has been added yet.
		if let Some(weak) = state.visited.get(path) {
			return Ok(Either::Right(weak.clone()));
		}

		// Get the root.
		let root = state.roots.iter().find_map(|(root, node)| {
			let diff = path.diff(root);
			diff.map_or(false, |diff| {
				matches!(
					diff.components().next(),
					Some(std::path::Component::CurDir | std::path::Component::Normal(_))
				)
			})
			.then_some((root.clone(), node.clone()))
		});

		// This should never happen, but if it does it means the code is incorrect higher up.
		if let Some((root_path, _node)) = &root {
			if root_path == path {
				return Err(tg::error!(%path = path.display(), "multiple instances of a root"));
			}
		}

		// If this isn't a root, get the parent.
		let parent = if root.is_none() {
			None
		} else {
			let referrer = referrer
				.ok_or_else(|| tg::error!(%path = path.display(), "expected a referrer"))?;
			let parent = match referrer {
				Either::Left(strong) => Arc::downgrade(&strong),
				Either::Right(weak) => weak.clone(),
			};
			Some(parent)
		};

		// Get the file system metadata.
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let metadata = tokio::fs::symlink_metadata(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the file's metadata"),
		)?;

		// Validate.
		if !(metadata.is_dir() || metadata.is_file() || metadata.is_symlink()) {
			return Err(tg::error!(%path = path.display(), "invalid file type"));
		}

		// Create the node.
		let path = path.to_owned();
		let node = Arc::new(RwLock::new(Graph {
			arg: tg::artifact::checkin::Arg {
				path: path.clone(),
				..arg.clone()
			},
			edges: Vec::new(),
			root: root.as_ref().map(|(path, _)| path.clone()),
			is_direct_dependency: false,
			metadata,
			lockfile: None,
			parent,
		}));

		// Add to the roots if necessary.
		if root.is_none() {
			state.roots.insert(path.clone(), node.clone());
		}

		// Update the state.
		state.visited.insert(path, Arc::downgrade(&node));

		Ok(Either::Left(node))
	}

	async fn get_or_create_intermediate_input_node(
		&self,
		referrer: Arc<RwLock<Graph>>,
		name: String,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
	) -> tg::Result<Node> {
		if !referrer.read().await.metadata.is_dir() {
			return Err(
				tg::error!(%path = referrer.read().await.arg.path.display(), "expected a directory"),
			);
		}

		let child = 'a: {
			// Check for an existing child.
			let reference = tg::Reference::with_path(&name);
			let child = referrer
				.read()
				.await
				.edges
				.iter()
				.find(|edge| edge.reference == reference)
				.map(|edge| {
					edge.graph
						.clone()
						.ok_or_else(|| tg::error!("expected a child node"))
				})
				.transpose()?;
			if let Some(child) = child {
				break 'a child;
			};

			// Get the referrer's path and root.
			let referrer_path = referrer.read().await.arg.path.clone();
			let referrer_root = referrer.read().await.root.clone();

			// Lock the state to avoid TOCTOU races.
			let mut state = state.write().await;

			// Create the absolute path.
			let path = referrer_path.join(name);

			// Check if a node has already been created.
			if let Some(node) = state.visited.get(&path) {
				break 'a Either::Right(node.clone());
			}

			// Add the referrer as a root if necessary.
			if referrer_root.is_none() {
				state.roots.insert(referrer_path.clone(), referrer.clone());
			}

			// Get the root path.
			let root = referrer_root.unwrap_or_else(|| referrer_path.clone());

			// Get the metadata.
			let metadata = tokio::fs::symlink_metadata(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to get metadata"),
			)?;

			// Create the child node.
			let child = Arc::new(RwLock::new(Graph {
				arg: tg::artifact::checkin::Arg {
					path: path.clone(),
					..arg.clone()
				},
				edges: Vec::new(),
				root: Some(root),
				is_direct_dependency: false,
				metadata,
				lockfile: None,
				parent: Some(Arc::downgrade(&referrer)),
			}));

			// Update state.
			state.visited.insert(path.clone(), Arc::downgrade(&child));
			drop(state);

			// Update the referrer.
			let edge = Edge {
				reference,
				graph: Some(Either::Left(child.clone())),
				object: None,
				tag: None,
			};
			add_edge(referrer, edge).await;

			Either::Right(Arc::downgrade(&child))
		};

		// Resolve any symlinks.
		self.try_resolve_symlink_node(child)
			.await?
			.ok_or_else(|| tg::error!("failed to resolve symlinks"))
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
			crate::util::lockfile::try_get_lockfile_node_for_module_path(path.as_ref())
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
	async fn print(self_: Arc<RwLock<Graph>>) {
		let mut stack = vec![(Either::Left(self_), 0)];
		while let Some((node, depth)) = stack.pop() {
			for _ in 0..depth {
				eprint!("..");
			}
			match node {
				Either::Left(strong) => {
					let path = strong.read().await.arg.path.clone();
					eprintln!("strong {path:#?} {:x?}", Arc::as_ptr(&strong));
					stack.extend(
						strong
							.read()
							.await
							.edges
							.iter()
							.filter_map(|edge| Some((edge.graph.clone()?, depth + 1))),
					);
				},
				Either::Right(weak) => {
					let path = weak.upgrade().unwrap().read().await.arg.path.clone();
					eprintln!("weak {path:#?} {:x?}", Weak::as_ptr(&weak));
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
			if node.read().await.root.is_none() {
				continue;
			}
			let parent = path.parent().ok_or_else(
				|| tg::error!(%path = path.display(), "expected path to have a parent"),
			)?;
			let root = node.read().await.root.clone().unwrap();
			let _parent = paths.get(parent)
				.ok_or_else(|| tg::error!(%root = root.display(), %path = path.display(), %parent = parent.display(), "missing parent node"))?;
		}

		Ok(())
	}
}

async fn add_edge(node: Arc<RwLock<Graph>>, edge: Edge) {
	let mut node = node.write().await;
	for existing_edge in &mut node.edges {
		if existing_edge.reference == edge.reference {
			if let Some(Either::Left(strong)) = &edge.graph {
				existing_edge.graph.replace(Either::Left(strong.clone()));
			}
			return;
		}
	}
	node.edges.push(edge);
}
