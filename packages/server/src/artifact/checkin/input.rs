use super::ignore;
use crate::{Server, lockfile::ParsedLockfile};
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Graph {
	pub root: PathBuf,
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug)]
pub struct Node {
	pub arg: tg::artifact::checkin::Arg,
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
	ignore_matcher: ignore::Matcher,
	graph: Graph,
	lockfiles: BTreeMap<PathBuf, Arc<ParsedLockfile>>,
	visited: BTreeMap<PathBuf, usize>,
}

impl Server {
	pub(super) async fn create_input_graph(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<Graph> {
		// Create the ignore matcher.
		let ignore_matcher = Self::ignore_matcher_for_checkin().await?;

		// Create the state.
		let state = RwLock::new(State {
			ignore_matcher,
			lockfiles: BTreeMap::new(),
			visited: BTreeMap::new(),
			graph: Graph {
				root: arg.path.clone(),
				nodes: Vec::new(),
			},
		});

		// Create the graph.
		Box::pin(self.create_input_graph_inner(None, arg.path.as_ref(), &arg, &state, progress))
			.await?;

		// Get the graph.
		let State { mut graph, .. } = state.into_inner();

		// Find roots and subpaths.
		graph.find_roots();
		graph.find_subpaths();

		// Validate the graph.
		graph.validate()?;

		Ok(graph)
	}

	fn create_input_graph_inner<'a>(
		&'a self,
		referrer: Option<usize>,
		path: &'a Path,
		arg: &'a tg::artifact::checkin::Arg,
		state: &'a RwLock<State>,
		progress: &'a crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> impl Future<Output = tg::Result<usize>> + Send + 'a {
		async move {
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
			let absolute_path = crate::util::fs::canonicalize_parent(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the parent"),
			)?;

			// Get the file system metadata.
			let metadata = tokio::fs::symlink_metadata(&absolute_path).await.map_err(|source| tg::error!(!source, %path = absolute_path.display(), "failed to get file system metadata"))?;

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
				.try_get_lockfile(&absolute_path, &metadata, &mut state_)
				.await?;

			// Create a new node.
			let arg_ = tg::artifact::checkin::Arg {
				path: absolute_path.clone(),
				..arg.clone()
			};
			let node = Node {
				arg: arg_,
				edges: Vec::new(),
				lockfile,
				metadata: metadata.clone(),
				parent,
			};
			state_.graph.nodes.push(node);
			let node = state_.graph.nodes.len() - 1;
			state_.visited.insert(absolute_path.clone(), node);

			// Get the list of dangling directories.
			let dangling = state_.get_dangling_directories(node);

			// Drop the state.
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
			progress.increment("input", 1);
			Ok(node)
		}
	}

	async fn try_get_lockfile(
		&self,
		path: &Path,
		metadata: &std::fs::Metadata,
		state: &mut State,
	) -> tg::Result<Option<Arc<ParsedLockfile>>> {
		// First check if the lockfile is set by this file.
		let lockfile_path = 'a: {
			if !metadata.is_file() {
				break 'a None;
			}
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let Ok(Some(_)) = xattr::get(path, tg::file::XATTR_LOCK_NAME) else {
				break 'a None;
			};
			Some(path.to_owned())
		};

		// Otherwise look up in the path's ancestors.
		let lockfile_path = 'a: {
			if let Some(path) = lockfile_path {
				break 'a Some(path);
			}
			for path in path.ancestors() {
				let path = path.join(tg::package::LOCKFILE_FILE_NAME);
				if matches!(tokio::fs::try_exists(&path).await, Ok(true)) {
					break 'a Some(path);
				}
			}
			None
		};

		// If none is found, break early.
		let Some(lockfile_path) = lockfile_path else {
			return Ok(None);
		};

		// Check if a lockfile has already been read.
		if let Some(lockfile) = state.lockfiles.get(&lockfile_path).cloned() {
			return Ok(Some(lockfile));
		}

		// Try to parse the lockfile.
		let Ok(Some(lockfile)) = self.try_parse_lockfile(&lockfile_path).await else {
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
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
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
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
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
					.ignore_matcher
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

		let mut vec = Vec::with_capacity(names.len());
		for name in names {
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
			vec.push(edge);
		}
		Ok(vec)
	}

	async fn get_file_edges(
		&self,
		referrer: usize,
		path: &Path,
		arg: &tg::artifact::checkin::Arg,
		state: &RwLock<State>,
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<Vec<Edge>> {
		// Get the lockfile if it exists.
		let lockfile = state.read().await.graph.nodes[referrer].lockfile.clone();

		// Get the set of unresolved references.
		let imports = 'a: {
			// If this is a module path, attempt to parse it.
			if !tg::package::is_module_path(path) {
				break 'a None;
			}

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
					let diagnostic = tg::Diagnostic {
						location: None,
						severity: tg::diagnostic::Severity::Error,
						message: format!("failed to analyze the module: {error}"),
					};
					progress.diagnostic(diagnostic);
					break 'a None;
				},
			};

			Some(analysis.imports)
		};

		// Pre-emptively pull tags for the set of imports.
		if let Some(imports) = &imports {
			let imports = imports.clone();
			let server = self.clone();
			tokio::spawn(async move {
				server
					.pull_tags_from_imports(imports)
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to pull tags"))
					.ok();
			});
		}

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
				.filter_map(|(reference, referent)| {
					let referent = tg::Referent {
						item: referent.item?,
						path: referent.path,
						subpath: referent.subpath,
						tag: referent.tag,
					};
					Some((reference, referent))
				})
				.collect::<BTreeMap<_, _>>();
			break 'a Some(references);
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
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
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

			// Get the absolute path of the referent.
			let absolute_path = crate::util::fs::canonicalize_parent(path.parent().unwrap().join(import_path)).await.map_err(|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path's parent"))?;

			// Get the referent.
			let child = self
				.create_input_graph_inner(Some(referrer), &absolute_path, arg, state, progress)
				.await
				.map_err(|source| tg::error!(!source, "failed to collect child input"))?;

			// If the child is a package and the import kind is not directory, get the subpath.
			let (child_path, is_directory) = {
				let state = state.read().await;
				(
					state.graph.nodes[child].arg.path.clone(),
					state.graph.nodes[child].metadata.is_dir(),
				)
			};

			// Get the subpath if this is a package import.
			let (node, subpath) = if matches!(&import.kind, Some(tg::module::Kind::Directory))
				|| !is_directory
			{
				(child, None)
			} else {
				let subpath = tg::package::try_get_root_module_file_name(self,Either::Right(&child_path)).await.map_err(|source| tg::error!(!source, %path = child_path.display(), "failed to get root module file name"))?
						.map(PathBuf::from);
				(child, subpath)
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
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
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
		_progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<Vec<Edge>> {
		// Check if this node's edges have already been created.
		let existing = state.read().await.graph.nodes[referrer].edges.clone();
		if !existing.is_empty() {
			return Ok(existing);
		}

		// Read the symlink.
		let target = tokio::fs::read_link(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to read symlink"),
		)?;

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
	async fn pull_tags_from_imports(
		&self,
		imports: impl IntoIterator<Item = tg::Import>,
	) -> tg::Result<()> {
		imports
			.into_iter()
			.filter_map(|import| {
				let server = self.clone();
				let remote = import
					.reference
					.options()
					.and_then(|options| options.remote.clone());
				let pattern = import.reference.item().try_unwrap_tag_ref().ok()?.clone();
				let future = async move { server.pull_tag(pattern, remote).await };
				Some(future)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}
}

impl Graph {
	// Find the roots of every node.
	fn find_roots(&mut self) {
		'outer: for node in 0..self.nodes.len() {
			// Find an ancestor.
			let ancestor = (0..self.nodes.len()).find(|ancestor| {
				// Cannot be an ancestor of self.
				if *ancestor == node {
					return false;
				}
				// Skip non-directories.
				if !self.nodes[*ancestor].metadata.is_dir() {
					return false;
				}
				self.nodes[node]
					.arg
					.path
					.strip_prefix(&self.nodes[*ancestor].arg.path)
					.is_ok()
			});

			// If there are no ancestors of this node, strip its parent.
			let Some(ancestor) = ancestor else {
				self.nodes[node].parent.take();
				continue 'outer;
			};

			// Otherwise, walk the ancestor's children to find the parent of the node.
			let mut stack = vec![ancestor];
			while let Some(parent) = stack.pop() {
				for child in self.nodes[parent].edges.iter().filter_map(|edge| edge.node) {
					if child == node {
						self.nodes[node].parent.replace(parent);
						continue 'outer;
					}
					if self.nodes[child].metadata.is_dir() {
						stack.push(child);
					}
				}
			}
		}
	}

	// Convert dependencies into root + subpath.
	fn find_subpaths(&mut self) {
		for node in 0..self.nodes.len() {
			// Only consider module imports.
			if !(self.nodes[node].metadata.is_file()
				&& tg::package::is_module_path(&self.nodes[node].arg.path))
			{
				continue;
			}

			// Remap edges.
			for edge_index in 0..self.nodes[node].edges.len() {
				// Skip edges that don't point to inputs.
				let Some(referent) = self.nodes[node].edges[edge_index].node else {
					continue;
				};

				// Skip edgs that have their subpath already set.
				if self.nodes[node].edges[edge_index].subpath.is_some() {
					continue;
				}

				// Get the root and subpath of the referent.
				let (package, subpath) = root_node_with_subpath(self, referent);

				// Get the path of the root.
				let path = crate::util::path::diff(&self.root, &self.nodes[package].arg.path).ok();

				// Create a new edge.
				let edge = Edge {
					kind: self.nodes[node].edges[edge_index].kind,
					node: Some(package),
					reference: self.nodes[node].edges[edge_index].reference.clone(),
					subpath,
					object: None,
					path,
					tag: None,
				};

				// Update the edge.
				self.nodes[node].edges[edge_index] = edge;
			}
		}
	}

	// Validate the input graph:
	fn validate(&self) -> tg::Result<()> {
		// Get the paths of all the nodes via DFS walk of the graph.
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

		// Validate that all parents are reachable.
		for (path, node) in &paths {
			let Some(parent) = self.nodes[*node].parent else {
				continue;
			};
			let parent = self.nodes[parent].arg.path.clone();
			let _parent = paths.get(&parent).ok_or_else(
				|| tg::error!(%path = path.display(), %parent = parent.display(), "missing parent node"),
			)?;
		}

		// Validate that no modules imported by other modules are outside of the same root.
		for (index, node) in self.nodes.iter().enumerate() {
			if !node.metadata.is_file() {
				continue;
			}

			// Get the root of this node.
			let root = get_root_node(self, index);
			for edge in &node.edges {
				let Some(child) = edge.node else {
					continue;
				};
				if !self.nodes[child].metadata.is_file()
					|| !tg::package::is_module_path(&self.nodes[child].arg.path)
				{
					continue;
				}
				let root_of_child = get_root_node(self, child);
				if root_of_child != root {
					return Err(tg::error!(
						%path = node.arg.path.display(),
						%reference = edge.reference,
						"cannot import external modules"
					));
				}
			}
		}

		Ok(())
	}
}

fn root_node_with_subpath(graph: &Graph, node: usize) -> (usize, Option<PathBuf>) {
	// Find the root node.
	let root = get_root_node(graph, node);

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
	fn get_dangling_directories(&mut self, node: usize) -> Vec<PathBuf> {
		// Get the root.
		let root = get_root_node(&self.graph, node);

		// Get the paths of this node and the root.
		let node_path = &self.graph.nodes[node].arg.path;
		let root_path = &self.graph.nodes[root].arg.path;

		// Check if this is a child of the root.
		let Ok(diff) = root_path.strip_prefix(node_path) else {
			return Vec::new();
		};

		// Collect any paths along the way that haven't been visited.
		let mut dangling = Vec::new();
		let mut path = root_path.to_owned();
		for component in diff.components() {
			path = path.join(component);
			if !self.visited.contains_key(&path) {
				dangling.push(path.clone());
			}
		}

		dangling
	}
}

fn get_root_node(graph: &Graph, mut node: usize) -> usize {
	loop {
		let Some(parent) = graph.nodes[node].parent else {
			return node;
		};
		node = parent;
	}
}

impl std::fmt::Display for Graph {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (index, node) in self.nodes.iter().enumerate() {
			let path = self.nodes[index].arg.path.clone();
			let path = crate::util::path::diff(&self.root, &path).unwrap();
			writeln!(f, "{index} {}", path.display())?;
			for edge in &node.edges {
				writeln!(f, "\t{edge}")?;
			}
		}
		Ok(())
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
