use crate::{
	util::module::{is_module_path, is_root_module_path, try_get_root_module_path_for_path},
	Server,
};
use futures::{
	stream::{self, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet, VecDeque},
	path::PathBuf,
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_either::Either;

#[derive(Clone, Debug)]
pub struct Graph {
	pub arg: tg::artifact::checkin::Arg,
	pub edges: Vec<Edge>,
	pub is_root: bool,
	pub is_direct_dependency: bool,
	pub metadata: std::fs::Metadata,
	pub lockfile: Option<(Arc<tg::Lockfile>, tg::Path)>,
	pub parent: Option<Weak<RwLock<Self>>>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub follow_symlinks: bool,
	pub unify_tags: bool,
	pub graph: Option<Arc<RwLock<Graph>>>,
	pub object: Option<tg::object::Id>,
}

struct State {
	roots: Vec<tg::Path>,
	visited: BTreeMap<PathBuf, Arc<RwLock<Graph>>>,
}

#[derive(Clone)]
struct Arg {
	arg: tg::artifact::checkin::Arg,
	follow_symlinks: bool,
	is_direct_dependency: bool,
	lockfile: Option<(Arc<tg::Lockfile>, tg::Path)>,
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
		let path = self.find_root(arg.path.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = arg.path, "failed to find root path for checkin"),
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
		let path = arg.path.parent().normalize();
		let arg_ = tg::artifact::checkin::Arg { path, ..arg };
		self.collect_input(arg_, progress).await
	}

	async fn find_root(&self, path: &std::path::Path) -> tg::Result<tg::Path> {
		if !path.is_absolute() {
			return Err(tg::error!(%path = path.display(), "expected an absolute path"));
		}

		if !is_module_path(path) {
			return path.to_owned().try_into();
		}

		// Look for a tangram.ts
		let path = path.parent().unwrap();
		for path in path.ancestors() {
			for root_module_name in tg::module::ROOT_MODULE_FILE_NAMES {
				let root_module_path = path.join(root_module_name);
				if tokio::fs::try_exists(&root_module_path).await.map_err(|source| tg::error!(!source, %root_module_path = root_module_path.display(), "failed to check if root module exists"))? {
					return path.to_owned().try_into();
				}
			}
		}

		// Otherwise return the parent.
		path.to_owned().try_into()
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
			lockfile: None,
			parent: None,
			root: 0,
			unify_tags: false,
		};
		let input = self.collect_input_inner(arg, &state, progress).await?;
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
	) -> tg::Result<Arc<RwLock<Graph>>> {
		let canonical_path = if arg.follow_symlinks {
			tokio::fs::canonicalize(&arg.arg.path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize path"))?
		} else {
			arg.arg.path.clone().normalize().to_string().into()
		};
		if let Some(visited) = state.read().unwrap().visited.get(&canonical_path).cloned() {
			return Ok(visited);
		}

		// Detect if this is a root or not.
		let is_root = state.read().unwrap().roots.iter().all(|root| {
			let first_component = root
				.diff(&arg.arg.path)
				.and_then(|p| p.components().first().cloned());
			!matches!(first_component, Some(tg::path::Component::Parent))
		});

		// Add to the roots if necessary.
		let root = if is_root {
			let mut state_ = state.write().unwrap();
			let root = state_.roots.len();
			state_.roots.push(arg.arg.path.clone());
			root
		} else {
			arg.root
		};

		// Get the file system metatadata.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let metadata = if arg.follow_symlinks {
			tokio::fs::symlink_metadata(&arg.arg.path).await.map_err(
				|source| tg::error!(!source, %path = arg.arg.path, "failed to get file metadata"),
			)?
		} else {
			tokio::fs::metadata(&arg.arg.path).await.map_err(
				|source| tg::error!(!source, %path = arg.arg.path, "failed to get file metadata"),
			)?
		};
		drop(permit);

		// Check if there's a lockfile.
		let lockfile = 'a: {
			// Try and re-use the parent if possible.
			if let Some((lockfile, root)) = arg.lockfile.clone() {
				break 'a Some((lockfile, root));
			}

			// Otherwise try and read locally.
			if let Some((lockfile, root)) = self.try_read_lockfile(&arg.arg.path, &metadata).await?
			{
				break 'a Some((Arc::new(lockfile), root));
			}

			if is_root_module_path(arg.arg.path.as_ref()) {
				break 'a Some((Arc::new(tg::Lockfile::default()), arg.arg.path.clone()));
			}

			None
		};

		// Create the input, without its dependencies.
		let input = Arc::new(RwLock::new(Graph {
			arg: tg::artifact::checkin::Arg {
				path: canonical_path.clone().try_into()?,
				..arg.arg.clone()
			},
			edges: Vec::new(),
			is_root,
			is_direct_dependency: arg.is_direct_dependency,
			lockfile: lockfile.clone(),
			metadata: metadata.clone(),
			parent: arg.parent.clone(),
		}));

		// Update state.
		state
			.write()
			.unwrap()
			.visited
			.insert(canonical_path, input.clone());

		// Get the edges.
		let arg = Arg {
			parent: Some(Arc::downgrade(&input)),
			root,
			..arg.clone()
		};

		let mut edges = self.get_edges(arg, state, progress, &metadata).await?;
		edges.sort_unstable_by_key(|edge| edge.reference.clone());

		input.write().unwrap().edges = edges;
		// Send a new progress report.
		progress.report_input_progress();

		Ok(input)
	}

	async fn try_read_lockfile(
		&self,
		path: &tg::Path,
		metadata: &std::fs::Metadata,
	) -> tg::Result<Option<(tg::Lockfile, tg::Path)>> {
		let mut root = if metadata.is_file() && is_root_module_path(path.as_ref()) {
			path.clone().parent().clone().normalize()
		} else {
			return Ok(None);
		};

		// Search
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let lockfile = loop {
			if root.components().len() == 1 {
				return Ok(None);
			}
			let lockfile_path = root.clone().join(tg::lockfile::TANGRAM_LOCKFILE_FILE_NAME);
			if let Some(lockfile) = tg::Lockfile::try_read(&lockfile_path).await? {
				break lockfile;
			}
			root = root.parent().normalize();
		};
		drop(permit);

		let Some(path) = root.diff(path) else {
			return Ok(None);
		};
		if !lockfile.paths.contains_key(&path) {
			return Ok(None);
		};
		Ok(Some((lockfile, root)))
	}

	async fn get_edges(
		&self,
		arg: Arg,
		state: &RwLock<State>,
		progress: &super::ProgressState,
		metadata: &std::fs::Metadata,
	) -> tg::Result<Vec<Edge>> {
		if metadata.is_dir() {
			if let Some(root_module_path) =
				try_get_root_module_path_for_path(arg.arg.path.as_ref()).await?
			{
				let follow_symlinks = arg.follow_symlinks;
				let unify_tags = arg.unify_tags;
				let mut arg = arg.clone();
				arg.arg.path = arg
					.arg
					.path
					.clone()
					.join(root_module_path.clone())
					.normalize();
				let graph = Box::pin(self.collect_input_inner(arg, state, progress)).await?;
				let edge = Edge {
					reference: tg::Reference::with_path(&root_module_path),
					follow_symlinks,
					unify_tags,
					graph: Some(graph),
					object: None,
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
			let reference = tg::Reference::with_path(&name.into());
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
			let dependencies: Either<Vec<tg::object::Id>, BTreeMap<tg::Reference, tg::object::Id>> =
				serde_json::from_slice(&data)
					.map_err(|source| tg::error!(!source, "failed to deserialize xattr"))?;
			let edges = match dependencies {
				Either::Left(set) => set
					.into_iter()
					.map(|id| Edge {
						reference: tg::Reference::with_object(&id),
						follow_symlinks: arg.follow_symlinks,
						unify_tags: arg.unify_tags,
						graph: None,
						object: Some(id),
					})
					.collect(),
				Either::Right(map) => {
					map.into_iter()
						.map(|(reference, id)| async {
							let path = reference
								.path()
								.try_unwrap_path_ref()
								.ok()
								.or_else(|| reference.query()?.path.as_ref());
							let graph = if let Some(path) = path {
								// TODO: once path.parent() is implemented correctly, use it.
								let mut components = arg.arg.path.components().to_vec();
								components.pop();
								components.extend(path.clone().into_components());
								let path = tg::Path::with_components(components);

								// Follow the path.
								let mut arg_ = arg.arg.clone();
								arg_.path = path;
								let arg = Arg {
									arg: arg_,
									follow_symlinks: arg.follow_symlinks,
									unify_tags: arg.unify_tags,
									is_direct_dependency: true,
									..arg.clone()
								};
								let graph =
									Box::pin(self.collect_input_inner(arg, state, progress))
										.await?;

								Some(graph)
							} else {
								None
							};
							let edge = Edge {
								reference,
								follow_symlinks: arg.follow_symlinks,
								unify_tags: arg.unify_tags,
								graph,
								object: Some(id),
							};

							Ok::<_, tg::Error>(edge)
						})
						.collect::<FuturesUnordered<_>>()
						.try_collect()
						.await?
				},
			};
			return Ok(edges);
		}

		if is_module_path(arg.arg.path.as_ref()) || is_root_module_path(arg.arg.path.as_ref()) {
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
					let follow_symlinks = true; // TODO: where to put follow symlinks?
					let unify_tags = import
						.reference
						.query()
						.and_then(|query| query.unify)
						.unwrap_or(arg.unify_tags);

					// Follow path dependencies.
					let path = import
						.reference
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| import.reference.query()?.path.as_ref());
					if let Some(path) = path {
						// Create the reference
						let reference = tg::Reference::with_path(&path);

						// TODO: once path.parent() is implemented correctly, use it.
						let mut components = arg.arg.path.components().to_vec();
						components.pop();
						components.extend(path.clone().into_components());
						let path = tg::Path::with_components(components);

						// Check if the import points outside the package.
						let root_path = state.read().unwrap().roots[arg.root].clone();
						if path.diff(&root_path).unwrap().is_external()
							&& is_module_path(path.as_ref())
						{
							return Err(tg::error!("cannot import module outside of the package"));
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
						};

						return Ok(edge);
					}

					Ok(Edge {
						reference: import.reference,
						follow_symlinks,
						unify_tags,
						graph: None,
						object: None,
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
					let child = edge.graph.clone()?;
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
			let children = edges.into_iter().filter_map(|edge| edge.graph);
			queue.extend(children);
		}
		Ok(())
	}

	async fn reparent_file_path_dependency(&self, child: Arc<RwLock<Graph>>) -> tg::Result<()> {
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
				tg::error!(%parent = parent.read().unwrap().arg.path, %grandparent = grandparent.read().unwrap().arg.path, "expected a directory"),
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
		let mut components = diff.components().iter().cloned().collect::<VecDeque<_>>();

		// Walk up.
		while let Some(tg::path::Component::Parent) = components.front() {
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
					tg::error!(%path = new_parent.read().unwrap().arg.path, "expected a directory"),
				);
			}
			parent = new_parent;
		}

		// Walk down.
		while let Some(component) = components.pop_front() {
			match component {
				tg::path::Component::Current => (),
				tg::path::Component::Normal(name) if components.is_empty() => {
					let reference = tg::Reference::with_path(&name.parse()?);
					let edge = Edge {
						reference,
						follow_symlinks: true,
						unify_tags: false,
						graph: Some(child.clone()),
						object: None,
					};
					parent.write().unwrap().edges.push(edge);
					child
						.write()
						.unwrap()
						.parent
						.replace(Arc::downgrade(&parent));
					return Ok(());
				},
				tg::path::Component::Normal(name) => {
					let reference = tg::Reference::with_path(&name.parse()?);
					let input = parent
						.read()
						.unwrap()
						.edges
						.iter()
						.find(|edge| &edge.reference == &reference)
						.and_then(|edge| edge.graph.clone());
					if let Some(input) = input {
						parent = input;
						continue;
					}
					let arg = parent.read().unwrap().arg.clone();
					let path = arg.path.clone().join(&name).normalize();
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
						graph: Some(graph.clone()),
						object: None,
					};
					parent.write().unwrap().edges.push(edge);
					parent = graph;
				},
				_ => return Err(tg::error!(%diff, "unexpected path component")),
			}
		}

		Err(tg::error!("failed to reparent orphaned input node"))
	}
}

impl Graph {
	fn contains_path(&self, path: &tg::Path) -> bool {
		if &self.arg.path == path {
			return true;
		}
		self.edges
			.iter()
			.filter_map(|edge| edge.graph.as_ref())
			.any(|child| child.read().unwrap().contains_path(path))
	}
}
