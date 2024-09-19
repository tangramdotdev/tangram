use crate::Server;
use futures::{
	stream::{self, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet, VecDeque},
	path::{Path, PathBuf},
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_either::Either;
use tg::path::Ext as _;

#[derive(Clone, Debug)]
pub struct Graph {
	pub arg: tg::artifact::checkin::Arg,
	pub edges: Vec<Edge>,
	pub is_root: bool,
	pub is_direct_dependency: bool,
	pub metadata: std::fs::Metadata,
	pub lockfile: Option<(Arc<tg::Lockfile>, usize)>,
	pub parent: Option<Weak<RwLock<Self>>>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub follow_symlinks: bool,
	pub unify_tags: bool,
	pub graph: Option<Node>,
	pub object: Option<tg::object::Id>,
	pub tag: Option<tg::Tag>,
}

pub type Node = Either<Arc<RwLock<Graph>>, Weak<RwLock<Graph>>>;

struct State {
	roots: Vec<PathBuf>,
	visited: BTreeMap<PathBuf, Weak<RwLock<Graph>>>,
}

#[derive(Clone)]
struct Arg {
	#[allow(clippy::struct_field_names)]
	arg: tg::artifact::checkin::Arg,
	follow_symlinks: bool,
	is_direct_dependency: bool,
	parent: Option<Weak<RwLock<Graph>>>,
	root: usize,
	unify_tags: bool,
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
		if graph.read().unwrap().contains_path(&arg.path) {
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

		// Look for a tangram.ts
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
		let state = RwLock::new(State {
			roots: Vec::new(),
			visited: BTreeMap::new(),
		});
		let arg = Arg {
			arg,
			follow_symlinks: false,
			is_direct_dependency: false,
			parent: None,
			root: 0,
			unify_tags: false,
		};
		let input = self
			.collect_input_inner(arg.clone(), &state, progress)
			.await?
			.unwrap_left();
		if input.read().unwrap().metadata.is_dir() {
			self.reparent_file_path_dependencies(input.clone()).await?;
		}
		Ok(input)
	}

	#[allow(clippy::too_many_arguments)]
	async fn collect_input_inner(
		&self,
		arg: Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Node> {
		if !arg.arg.path.is_absolute() {
			let referrer = arg.parent.as_ref().map(|p| {
				p.upgrade()
					.unwrap()
					.read()
					.unwrap()
					.arg
					.path
					.display()
					.to_string()
			});
			return Err(
				tg::error!(%path = arg.arg.path.display(), ?referrer, "expected an absolute path"),
			);
		}
		let canonical_path = if arg.follow_symlinks {
			tokio::fs::canonicalize(&arg.arg.path).await.map_err(
				|source| tg::error!(!source, %path = arg.arg.path.display(), "failed to canonicalize path"),
			)?
		} else {
			arg.arg.path.normalize()
		};

		if let Some(visited) = state.read().unwrap().visited.get(&canonical_path).cloned() {
			return Ok(Either::Right(visited));
		}

		// Detect if this is a root or not.
		let is_root = !state.read().unwrap().roots.iter().any(|root| {
			let diff = canonical_path.diff(root).unwrap();
			matches!(
				diff.components().next(),
				Some(std::path::Component::CurDir | std::path::Component::Normal(_))
			)
		});

		// Add to the roots if necessary.
		let root = if is_root {
			let mut state_ = state.write().unwrap();
			let root = state_.roots.len();
			state_.roots.push(canonical_path.clone());
			root
		} else {
			arg.root
		};

		// Get the file system metatadata.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let metadata = if arg.follow_symlinks {
			tokio::fs::metadata(&arg.arg.path).await.map_err(
				|source| tg::error!(!source, %path = arg.arg.path.display(), "failed to get file metadata"),
			)?
		} else {
			tokio::fs::symlink_metadata(&arg.arg.path).await.map_err(
				|source| tg::error!(!source, %path = arg.arg.path.display(), "failed to get file metadata"),
			)?
		};
		drop(permit);

		// Create the input, without its dependencies.
		let input = Arc::new(RwLock::new(Graph {
			arg: tg::artifact::checkin::Arg {
				path: canonical_path.clone(),
				..arg.arg.clone()
			},
			edges: Vec::new(),
			is_root,
			is_direct_dependency: arg.is_direct_dependency,
			lockfile: None,
			metadata: metadata.clone(),
			parent: arg.parent.clone(),
		}));

		// Update state.
		state
			.write()
			.unwrap()
			.visited
			.insert(canonical_path.clone(), Arc::downgrade(&input));

		// Get the edges.
		let arg_ = Arg {
			arg: tg::artifact::checkin::Arg {
				path: canonical_path,
				..arg.arg.clone()
			},
			parent: Some(Arc::downgrade(&input)),
			root,
			..arg.clone()
		};

		let mut edges = self.get_edges(arg_, state, progress, &metadata).await?;
		edges.sort_unstable_by_key(|edge| edge.reference.clone());

		input.write().unwrap().edges = edges;

		// Send a new progress report.
		progress.report_input_progress();

		Ok(Either::Left(input))
	}

	async fn get_edges(
		&self,
		arg: Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
		metadata: &std::fs::Metadata,
	) -> tg::Result<Vec<Edge>> {
		if metadata.is_dir() {
			if let Some(root_module_file_name) =
				tg::package::try_get_root_module_file_name_for_package_path(arg.arg.path.as_ref())
					.await?
			{
				let follow_symlinks = arg.follow_symlinks;
				let unify_tags = arg.unify_tags;
				let mut arg = arg.clone();
				arg.arg.path = arg.arg.path.join(root_module_file_name).normalize();
				let graph = Box::pin(self.collect_input_inner(arg, state, progress)).await?;
				let edge = Edge {
					reference: tg::Reference::with_path(root_module_file_name),
					follow_symlinks,
					unify_tags,
					graph: Some(graph),
					object: None,
					tag: None,
				};
				return Ok(vec![edge]);
			}
			self.get_directory_edges(arg, state, progress).await
		} else if metadata.is_file() {
			self.get_file_edges(arg, state, progress).await
		} else if metadata.is_symlink() {
			Ok(Vec::new())
		} else {
			Err(tg::error!("invalid file type"))
		}
	}

	async fn get_directory_edges(
		&self,
		arg: Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Vec<Edge>> {
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let entries = tokio::fs::read_dir(&arg.arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read directory"))?;
		let vec = stream::try_unfold(entries, |mut entries| async {
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
			let path = arg.arg.path.clone().join(&name);

			// Create edge metadata.
			let reference = tg::Reference::with_path(&name);
			let follow_symlinks = arg.follow_symlinks;
			let unify_tags = arg.unify_tags;

			// Follow the edge.
			let mut arg = arg.clone();
			arg.arg.path = path;
			let graph = Box::pin(self.collect_input_inner(arg, state, progress)).await?;

			// Create the edge.
			let edge = Edge {
				reference,
				follow_symlinks,
				unify_tags,
				graph: Some(graph),
				object: None,
				tag: None,
			};
			Ok::<_, tg::Error>(Some((edge, entries)))
		})
		.collect::<FuturesUnordered<_>>()
		.await
		.into_iter()
		.try_collect()?;
		Ok(vec)
	}

	async fn get_file_edges(
		&self,
		arg: Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Vec<Edge>> {
		if let Some(data) = xattr::get(&arg.arg.path, tg::file::XATTR_NAME)
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
					let root_path = state.read().unwrap().roots[arg.root].clone();
					async move {
						let id = dependency.object;
						let path = reference
							.path()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.query()?.path.as_ref())
							.map(|path| arg.arg.path.parent().unwrap().join(path));

						let follow_symlinks =
							reference.query().and_then(|q| q.follow).unwrap_or(false);
						let unify_tags = true;

						// Don't follow paths that point outside the root.
						let is_external_path = path
							.as_ref()
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
							// Follow the path.
							let mut arg_ = arg.arg.clone();
							arg_.path = path;
							let arg = Arg {
								arg: arg_,
								follow_symlinks,
								unify_tags,
								is_direct_dependency: true,
								..arg.clone()
							};

							let graph =
								Box::pin(self.collect_input_inner(arg, state, progress)).await?;

							Some(graph)
						} else {
							None
						};
						let edge = Edge {
							reference,
							follow_symlinks,
							unify_tags,
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

		if tg::package::is_module_path(arg.arg.path.as_ref()) {
			return self.get_module_edges(arg, state, progress).await;
		}

		Ok(Vec::new())
	}

	async fn get_module_edges(
		&self,
		arg: Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
	) -> tg::Result<Vec<Edge>> {
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let text = tokio::fs::read_to_string(&arg.arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read file contents"))?;
		drop(permit);
		let analysis = crate::compiler::Compiler::analyze_module(text)
			.map_err(|source| tg::error!(!source, "failed to analyze module"))?;

		let edges = analysis
			.imports
			.into_iter()
			.map(|import| {
				let arg = arg.clone();
				async move {
					let follow_symlinks = import
						.reference
						.query()
						.and_then(|query| query.follow)
						.unwrap_or(true);

					let unify_tags = true;

					// Follow path dependencies.
					let path = import
						.reference
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| import.reference.query()?.path.as_ref());
					if let Some(path) = path {
						// Create the reference
						let reference = import.reference.clone();

						// Create the path.
						let path = arg.arg.path.parent().unwrap().to_owned().join(path);

						// Check if the import points outside the package.
						let root_path = state.read().unwrap().roots[arg.root].clone();
						if path.diff(&root_path).map_or(false, |path| {
							matches!(
								path.components().next(),
								Some(std::path::Component::ParentDir)
							)
						}) && tg::package::is_module_path(path.as_ref())
						{
							return Err(
								tg::error!(%root = root_path.display(), %path = path.display(), "cannot import module outside of the package"),
							);
						}

						// Follow the path.
						let mut arg_ = arg.arg.clone();
						arg_.path = path;
						let arg = Arg {
							arg: arg_,
							follow_symlinks,
							unify_tags,
							is_direct_dependency: true,
							..arg.clone()
						};
						let graph =
							Box::pin(self.collect_input_inner(arg, state, progress)).await?;

						// Create the edge.
						let edge = Edge {
							reference,
							follow_symlinks,
							unify_tags,
							graph: Some(graph),
							object: None,
							tag: None,
						};

						return Ok(edge);
					}

					Ok(Edge {
						reference: import.reference,
						follow_symlinks,
						unify_tags,
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

	async fn reparent_file_path_dependencies(&self, input: Arc<RwLock<Graph>>) -> tg::Result<()> {
		let mut visited = BTreeSet::new();
		let mut queue: VecDeque<_> = vec![input].into();

		// We explicitly use breadth-first traversal here as a heuristic, to ensure that most nodes' parents are already reparented.
		while let Some(input) = queue.pop_front() {
			// Check if we've visited this node.
			let absolute_path = input.read().unwrap().arg.path.clone();
			if visited.contains(&absolute_path) {
				continue;
			}
			visited.insert(absolute_path.clone());

			// Get the dependencies.
			let edges = input.read().unwrap().edges.clone();

			// If this input is a file, reparent its children.
			if input.read().unwrap().metadata.is_file() {
				let dependencies = edges.iter().filter_map(|edge| {
					let child = edge.node()?;
					edge.reference
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| edge.reference.query()?.path.as_ref())
						.map(|_| child)
				});
				for child in dependencies {
					self.reparent_file_path_dependency(child).await?;
				}
			}

			// Add its children to the stack.
			let children = edges.into_iter().filter_map(|edge| edge.node());
			queue.extend(children);
		}
		Ok(())
	}

	async fn reparent_file_path_dependency(&self, child: Arc<RwLock<Graph>>) -> tg::Result<()> {
		if child.read().unwrap().is_root {
			return Ok(());
		}
		let Some(parent) = child.read().unwrap().parent.as_ref().map(Weak::upgrade) else {
			return Ok(());
		};
		let parent = parent.unwrap();

		// Check if this node has already been reparented.
		if parent.read().unwrap().metadata.is_dir() {
			return Ok(());
		}

		// Reparent the parent. This short circuits if the parent is already re-parented.
		Box::pin(self.reparent_file_path_dependency(parent.clone())).await?;

		// Otherwise get the grandparent.
		let grandparent = parent
			.read()
			.unwrap()
			.parent
			.clone()
			.ok_or_else(|| tg::error!("invalid path"))?
			.upgrade()
			.unwrap();
		if !grandparent.read().unwrap().metadata.is_dir() {
			return Err(
				tg::error!(%parent = parent.read().unwrap().arg.path.display(), %grandparent = grandparent.read().unwrap().arg.path.display(), "expected a directory"),
			);
		}
		let mut parent = grandparent;

		// Get the path to the child relative to the parent.
		let diff = child
			.read()
			.unwrap()
			.arg
			.path
			.diff(&parent.read().unwrap().arg.path)
			.unwrap();
		let mut components = diff.components().collect::<VecDeque<_>>();

		// Walk up.
		while let Some(std::path::Component::ParentDir) = components.front() {
			components.pop_front();
			let new_parent = parent
				.read()
				.unwrap()
				.parent
				.clone()
				.ok_or_else(|| tg::error!("invalid path"))?
				.upgrade()
				.unwrap();
			if !new_parent.read().unwrap().metadata.is_dir() {
				return Err(
					tg::error!(%path = new_parent.read().unwrap().arg.path.display(), "expected a directory"),
				);
			}
			parent = new_parent;
		}

		// Walk down.
		while let Some(component) = components.pop_front() {
			match component {
				std::path::Component::CurDir => (),
				std::path::Component::Normal(name) if components.is_empty() => {
					let name = name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))?;
					let reference = tg::Reference::with_path(name);
					let edge = Edge {
						reference,
						follow_symlinks: true,
						unify_tags: false,
						graph: Some(Either::Right(Arc::downgrade(&child))),
						object: None,
						tag: None,
					};
					parent.write().unwrap().edges.push(edge);
					child
						.write()
						.unwrap()
						.parent
						.replace(Arc::downgrade(&parent));
					return Ok(());
				},
				std::path::Component::Normal(name) => {
					let name = name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))?;
					let reference = tg::Reference::with_path(name);
					let input = parent
						.read()
						.unwrap()
						.edges
						.iter()
						.find(|edge| edge.reference == reference)
						.and_then(Edge::node);
					if let Some(input) = input {
						parent = input;
						continue;
					}
					let arg = parent.read().unwrap().arg.clone();
					let path = arg.path.clone().join(name).normalize();
					let metadata = tokio::fs::metadata(&path).await.map_err(|source| {
						tg::error!(!source, "failed to get directory metadata")
					})?;
					let arg = tg::artifact::checkin::Arg { path, ..arg };
					let graph = Arc::new(RwLock::new(Graph {
						arg,
						edges: Vec::new(),
						is_root: false,
						is_direct_dependency: false,
						metadata,
						lockfile: parent.read().unwrap().lockfile.clone(),
						parent: Some(Arc::downgrade(&parent)),
					}));
					let edge = Edge {
						reference,
						follow_symlinks: true,
						unify_tags: false,
						graph: Some(Either::Left(graph.clone())),
						object: None,
						tag: None,
					};
					parent.write().unwrap().edges.push(edge);
					parent = graph;
				},
				_ => return Err(tg::error!(%diff = diff.display(), "unexpected path component")),
			}
		}

		Err(tg::error!("failed to reparent orphaned input node"))
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
		let path = input.read().unwrap().arg.path.clone();
		if visited.read().unwrap().contains(&path) {
			return Ok(());
		}

		// Mark this node as visited.
		visited.write().unwrap().insert(path.clone());

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
			input
				.read()
				.unwrap()
				.parent
				.as_ref()
				.map(|parent| parent.upgrade().unwrap())
				.and_then(|parent| parent.read().unwrap().lockfile.clone())
		};
		input.write().unwrap().lockfile = lockfile;

		// Recurse
		let edges = input.read().unwrap().edges.clone();
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
	fn contains_path(&self, path: &Path) -> bool {
		let mut visited = BTreeSet::new();
		self.contains_path_inner(path, &mut visited)
	}

	fn contains_path_inner<'a>(&self, path: &'a Path, visited: &mut BTreeSet<&'a Path>) -> bool {
		if visited.contains(&path) {
			return false;
		}
		if self.arg.path == path {
			return true;
		}
		visited.insert(path);
		self.edges
			.iter()
			.filter_map(Edge::node)
			.any(|child| child.read().unwrap().contains_path(path))
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
