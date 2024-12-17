use crate::{lockfile::ParsedLockfile, Server};
use futures::{stream::FuturesUnordered, Future, TryStreamExt as _};
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
	pub data: Option<tg::artifact::Data>,
	pub edges: Vec<Edge>,
	pub lockfile: Option<Arc<ParsedLockfile>>,
	pub metadata: std::fs::Metadata,
	pub parent: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub kind: Option<tg::module::Kind>,
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
	lockfiles: BTreeMap<PathBuf, Arc<ParsedLockfile>>,
	visited: BTreeMap<PathBuf, usize>,
}

impl Server {
	pub(super) async fn create_input_graph(
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
			lockfiles: BTreeMap::new(),
			visited: BTreeMap::new(),
			graph: Graph { nodes: Vec::new() },
		});

		// Create the graph.
		Box::pin(self.create_input_graph_inner(None, arg.path.as_ref(), &arg, &state, progress))
			.await?;

		// Validate the input graph.
		let State { graph, .. } = state.into_inner();
		graph.validate().await?;

		Ok(graph)
	}

	fn create_input_graph_inner<'a>(
		&'a self,
		referrer: Option<usize>,
		path: &'a Path,
		arg: &'a tg::artifact::checkin::Arg,
		state: &'a RwLock<State>,
		progress: Option<&'a crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> impl Future<Output = tg::Result<usize>> + Send + 'a {
		let future = async move {
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

			// Get the file system metadata.
			let metadata = tokio::fs::symlink_metadata(&absolute_path).await.map_err(
			|source| tg::error!(!source, %path = absolute_path.display(), "failed to get file system metadata"),
		)?;
			drop(permit);

			// Validate.
			if !(metadata.is_dir() || metadata.is_file() || metadata.is_symlink()) {
				return Err(tg::error!(%path = absolute_path.display(), "invalid file type"));
			}

			// If this is a root module file, ensure the parent is collected.
			if metadata.is_file() && tg::package::is_root_module_path(&absolute_path) {
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

			// Try to get the lockfile.
			let lockfile = self
				.try_get_lockfile_for_path(&absolute_path, &mut state_)
				.await?;

			// Create a new node.
			let arg_ = tg::artifact::checkin::Arg {
				path: absolute_path.clone(),
				..arg.clone()
			};
			let node = Node {
				arg: arg_,
				data: None,
				edges: Vec::new(),
				lockfile,
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
				.get_edges(node, &absolute_path, arg, state, metadata, progress)
				.await
				.map_err(
					|source| tg::error!(!source, %path = absolute_path.display(), "failed to get edges"),
				)?;

			// Update the node.
			state.write().await.graph.nodes[node].edges = edges;

			// Return the created node.
			Ok(node)
		};
		Box::pin(future)
	}

	async fn try_get_lockfile_for_path(
		&self,
		path: &Path,
		state: &mut State,
	) -> tg::Result<Option<Arc<ParsedLockfile>>> {
		let mut lockfile_path = None;
		for path in path.ancestors() {
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let path = path.join(tg::package::LOCKFILE_FILE_NAME);
			if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
				lockfile_path.replace(path);
				break;
			}
		}
		let Some(lockfile_path) = lockfile_path else {
			return Ok(None);
		};

		// Check if a lockfile exists.
		if let Some(lockfile) = state.lockfiles.get(&lockfile_path).cloned() {
			return Ok(Some(lockfile));
		}

		// Try to parse the lockfile.
		let Ok(lockfile) = self.parse_lockfile(&lockfile_path).await else {
			return Ok(None);
		};
		let lockfile = Arc::new(lockfile);

		// Update state.
		state
			.lockfiles
			.insert(lockfile_path.clone(), lockfile.clone());

		// Return the created lockfile.
		Ok(Some(lockfile))
	}

	async fn get_edges(
		&self,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		metadata: std::fs::Metadata,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Vec<Edge>> {
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
						kind: None,
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
		// Get the lockfile if it exists.
		let lockfile = state.read().await.graph.nodes[referrer].lockfile.clone();

		// Get the set of unresolved references.
		let imports = 'a: {
			// If this is a module path, attempt to parse it.
			if !tg::package::is_module_path(path) {
				break 'a None;
			};

			// Read the file, returning an error if we couldn't.
			let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let text = tokio::fs::read_to_string(path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to read file contents"),
			)?;
			drop(permit);

			// Analyze the module, forwarding errors as diagnostics.
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
					break 'a None;
				},
			};

			Some(analysis.imports)
		};

		// Try and get the references of this file in the lockfile, if it exists.
		let resolved_references = 'a: {
			// Break if no lockfile is present.
			let Some(lockfile) = lockfile else {
				break 'a None;
			};

			// Break if we can't find the dependencies of this file.
			let Ok(references) = lockfile.get_file_dependencies(path) else {
				break 'a None;
			};

			// Collect the references from the lockfile.
			let references = references
				.into_iter()
				.map(|(reference, referent)| {
					let referent = tg::Referent {
						item: Either::Left(referent.item),
						path: referent.path,
						subpath: referent.subpath,
						tag: referent.tag,
					};
					(reference, referent)
				})
				.collect::<BTreeMap<_, _>>();
			break 'a Some(references);
		};

		// Try and get the references of this file from xattrs, if they exist.
		let resolved_references = 'a: {
			// Prefer the resolved references read from the lockfile.
			if let Some(references) = resolved_references {
				break 'a Some(references);
			};

			// Try and read the xattr if it exists.
			let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let Ok(Some(xattr)) = xattr::get(path, tg::file::XATTR_DATA_NAME) else {
				break 'a None;
			};
			drop(permit);

			// Try and deserialize the xattr, skipping if it can't be read.
			let Ok(data) = tg::file::Data::deserialize(&xattr.into()) else {
				break 'a None;
			};

			// Try to get the dependencies from the xattr data.
			match data {
				tg::file::Data::Graph { graph, node } => {
					// Skip trying to get dependencies if the graph object doesn't exist.
					let Ok(dependencies) =
						tg::File::with_graph_and_node(tg::Graph::with_id(graph), node)
							.dependencies(self)
							.await
					else {
						break 'a None;
					};

					// Collect the references by getting their IDs.
					let mut dependencies_ = BTreeMap::new();
					for (reference, referent) in dependencies {
						let id = referent.item.id(self).await?;
						let referent = tg::Referent {
							item: Either::Right(id),
							path: referent.path,
							subpath: referent.subpath,
							tag: referent.tag,
						};
						dependencies_.insert(reference, referent);
					}
					Some(dependencies_)
				},
				tg::file::Data::Normal { dependencies, .. } => {
					let dependencies = dependencies
						.into_iter()
						.map(|(reference, referent)| {
							let referent = tg::Referent {
								item: Either::Right(referent.item),
								path: referent.path,
								subpath: referent.subpath,
								tag: referent.tag,
							};
							(reference, referent)
						})
						.collect();
					Some(dependencies)
				},
			}
		};

		// Try and reconcile the resolved/unresolved references.
		match (imports, resolved_references) {
			// If both resolved references and imports are present, reconcile the two.
			(Some(imports), Some(resolved_references)) => {
				if arg.locked {
					let matches = resolved_references.len() == imports.len()
						&& imports
							.iter()
							.all(|import| resolved_references.contains_key(&import.reference));
					if !matches {
						return Err(tg::error!("lockfile is out of date"));
					}
				}

				let mut edges = Vec::with_capacity(imports.len());
				for import in imports {
					let edge = if let Some(referent) = resolved_references.get(&import.reference) {
						self.get_edge_from_path_or_id(
							import.reference.clone(),
							referent.clone(),
							referrer,
							path,
							arg,
							state,
							progress,
						)
						.await?
					} else {
						self.get_edge_from_import(import, referrer, path, arg, state, progress)
							.await?
					};
					edges.push(edge);
				}

				// Return the merged references
				Ok(edges)
			},

			// If only unresolved references exist, return them.
			(Some(imports), None) => {
				let mut edges = Vec::with_capacity(imports.len());
				for import in imports {
					let edge = self
						.get_edge_from_import(import, referrer, path, arg, state, progress)
						.await?;
					edges.push(edge);
				}
				Ok(edges)
			},

			// If only resolved references exist, return them.
			(None, Some(resolved_references)) => {
				let mut edges = Vec::with_capacity(resolved_references.len());
				for (reference, referent) in resolved_references {
					let edge = self
						.get_edge_from_path_or_id(
							reference, referent, referrer, path, arg, state, progress,
						)
						.await?;
					edges.push(edge);
				}
				Ok(edges)
			},

			// Otherwise return no references.
			(None, None) => Ok(Vec::new()),
		}
	}

	async fn get_edge_from_import(
		&self,
		import: tg::Import,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Edge> {
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
			let root_node = get_root_node(&state.read().await.graph, referrer).await;
			let root_path = state.read().await.graph.nodes[root_node].arg.path.clone();

			let absolute_path = crate::util::fs::canonicalize_parent(path.parent().unwrap().join(import_path)).await.map_err(|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path's parent"))?;
			let is_external = absolute_path.strip_prefix(&root_path).is_err();

			// If the import is of a module and points outside the root, return an error.
			if (import_path.is_absolute() || is_external)
				&& tg::package::is_module_path(import_path.as_ref())
			{
				return Err(
					tg::error!(%root = root_path.display(), %path = import_path.display(), "cannot import module outside of the package"),
				);
			}

			// Get the input of the referent.
			let child = if is_external {
				// If this is an external import, treat it as a root using the absolute path.
				self.create_input_graph_inner(Some(referrer), &absolute_path, arg, state, progress)
					.await
					.map_err(|source| tg::error!(!source, "failed to collect child input"))?
			} else {
				// Otherwise, treat it as a relative path.

				// Get the parent of the referrer.
				let parent = state.read().await.graph.nodes[referrer]
					.parent
					.ok_or_else(|| tg::error!("expected a parent"))?;

				// Recurse.
				self.create_input_graph_inner(
					Some(parent),
					import_path.as_ref(),
					arg,
					state,
					progress,
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to collect child input"))?
			};

			// If the child is a package and the import kind is not directory, get the subpath.
			let (child_path, is_directory) = {
				let state = state.read().await;
				(
					state.graph.nodes[child].arg.path.clone(),
					state.graph.nodes[child].metadata.is_dir(),
				)
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
				let (node, subpath) =
					root_node_with_subpath(&state.read().await.graph, child).await;

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
				kind: import.kind,
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
			kind: import.kind,
			reference: import.reference.clone(),
			subpath: None,
			node: None,
			object: None,
			path: None,
			tag: None,
		})
	}

	#[allow(clippy::too_many_arguments)]
	async fn get_edge_from_path_or_id(
		&self,
		reference: tg::Reference,
		referent: tg::Referent<Either<PathBuf, tg::object::Id>>,
		referrer: usize,
		_path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<Edge> {
		let node = if let Either::Left(path) = &referent.item {
			let node =
				Box::pin(self.create_input_graph_inner(Some(referrer), path, arg, state, progress))
					.await?;
			Some(node)
		} else {
			None
		};
		let object = if let Either::Right(object) = &referent.item {
			Some(object.clone())
		} else {
			None
		};
		let edge = Edge {
			kind: None,
			reference,
			subpath: referent.subpath,
			node,
			object,
			path: referent.path,
			tag: referent.tag,
		};
		Ok(edge)
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
		let artifacts_path = self.get_artifacts_path_for_path(path).await?;
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
			let subpath: PathBuf = components.collect();
			Edge {
				kind: None,
				reference: tg::Reference::with_path(target),
				subpath: Some(subpath),
				node: None,
				object: Some(object),
				path: Some(path),
				tag: None,
			}
		} else {
			Edge {
				kind: None,
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
