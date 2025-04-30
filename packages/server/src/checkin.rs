use crate::{Server, blob::create::Blob, lockfile::ParsedLockfile, temp::Temp};
use bytes::Bytes;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, stream::FuturesUnordered,
};
use indoc::indoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap,
	ops::Not as _,
	os::unix::fs::PermissionsExt as _,
	panic::AssertUnwindSafe,
	path::{Path, PathBuf},
	sync::Arc,
	time::Instant,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::prelude::*;
use tokio_util::task::AbortOnDropHandle;

pub(crate) mod ignore;
mod input;
mod lockfile;
mod object;
mod output;
mod unify;

struct State {
	arg: tg::checkin::Arg,
	graph: Graph,
	graph_objects: Vec<GraphObject>,
	lockfile: Option<ParsedLockfile>,
	locked: bool,
	ignorer: Option<tangram_ignore::Ignorer>,
	#[allow(dead_code)]
	progress: crate::progress::Handle<tg::checkin::Output>,
}

struct GraphObject {
	id: tg::graph::Id,
	data: tg::graph::Data,
	bytes: Bytes,
}

#[derive(Clone, Debug)]
pub struct Graph {
	nodes: Vec<Node>,
	paths: radix_trie::Trie<PathBuf, usize>,
	roots: BTreeMap<usize, Vec<usize>>,
}

#[derive(Clone, Debug)]
struct Node {
	#[allow(dead_code)]
	metadata: Option<std::fs::Metadata>,
	object: Option<Object>,
	path: Option<Arc<PathBuf>>,
	root: Option<usize>,
	tag: Option<tg::Tag>,
	variant: Variant,
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
enum Variant {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug)]
struct Directory {
	entries: Vec<(String, usize)>,
}

#[derive(Clone, Debug)]
struct File {
	blob: Option<Blob>,
	executable: bool,
	dependencies: Vec<FileDependency>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum FileDependency {
	Import {
		import: tg::Import,
		path: Option<PathBuf>,
		subpath: Option<PathBuf>,
		node: Option<usize>,
	},
	Referent {
		reference: tg::Reference,
		referent: tg::Referent<tg::object::Id>,
	},
}

#[derive(Clone, Debug)]
struct Symlink {
	target: PathBuf,
}

#[derive(Clone, Debug)]
struct Object {
	bytes: Bytes,
	data: tg::object::Data,
	id: tg::object::Id,
}

impl Server {
	pub async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = AssertUnwindSafe(server.checkin_inner(arg, &progress))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	// Check in the artifact.
	async fn checkin_inner(
		&self,
		mut arg: tg::checkin::Arg,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::checkin::Output> {
		// Canonicalize the path's parent.
		arg.path = crate::util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = &arg.path.display(), "failed to canonicalize the path's parent"))?;

		// If this is a checkin of a path in the cache directory, then retrieve the corresponding artifact.
		if let Ok(path) = arg.path.strip_prefix(self.cache_path()) {
			let id = path
				.components()
				.next()
				.map(|component| {
					let std::path::Component::Normal(name) = component else {
						return Err(tg::error!("invalid path"));
					};
					name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))
				})
				.ok_or_else(|| tg::error!("cannot check in the cache directory"))??
				.parse()?;
			if path.components().count() == 1 {
				let output = tg::checkin::Output { artifact: id };
				return Ok(output);
			}
			let path = path.components().skip(1).collect::<PathBuf>();
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, path).await?;
			let id = artifact.id(self).await?;
			let output = tg::checkin::Output { artifact: id };
			return Ok(output);
		}

		self.checkin_new(arg, progress).await
	}

	async fn checkin_new(
		&self,
		arg: tg::checkin::Arg,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::checkin::Output> {
		// Create the ignorer if necessary.
		let ignorer = if arg.ignore {
			Some(Self::checkin_create_ignorer()?)
		} else {
			None
		};

		// Search for the root.
		let root = tg::package::try_get_nearest_package_path_for_path(&arg.path)?
			.unwrap_or(&arg.path)
			.to_owned();

		// Parse a lockfile if it exists.
		let lockfile = self
			.try_parse_lockfile(&root)
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?;

		// Create the state.
		let graph = Graph {
			nodes: Vec::new(),
			paths: radix_trie::Trie::default(),
			roots: BTreeMap::new(),
		};
		let mut state = State {
			arg: arg.clone(),
			graph,
			graph_objects: Vec::new(),
			lockfile,
			locked: arg.locked,
			ignorer,
			progress: progress.clone(),
		};

		// Visit.
		let start = Instant::now();
		let mut state = tokio::task::spawn_blocking({
			let server = self.clone();
			move || {
				server.checkin_visit(&mut state, root)?;
				Ok::<_, tg::Error>(state)
			}
		})
		.await
		.unwrap()?;
		tracing::trace!(elapsed = ?start.elapsed(), "visit");

		// Remove the ignorer.
		state.ignorer.take();

		// Find roots.
		let start = Instant::now();
		Self::checkin_find_roots(&mut state);
		tracing::trace!(elapsed = ?start.elapsed(), "find roots");

		// Update subpaths.
		let start = Instant::now();
		Self::checkin_find_subpaths(&mut state);
		tracing::trace!(elapsed = ?start.elapsed(), "find subpaths");

		// Create blobs.
		let start = Instant::now();
		self.checkin_create_blobs(&mut state).await?;
		tracing::trace!(elapsed = ?start.elapsed(), "create blobs");

		// Create objects.
		let start = Instant::now();
		Self::checkin_create_objects(&mut state)?;
		tracing::trace!(elapsed = ?start.elapsed(), "create objects");

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Get the root node's ID.
		let root: tg::artifact::Id = state.graph.nodes[0]
			.object
			.as_ref()
			.unwrap()
			.id
			.clone()
			.try_into()
			.unwrap();

		// Write the objects to the database and the store.
		let state = Arc::new(state);
		let cache_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_cache_task(state, touched_at)
					.await
					.map_err(|source| tg::error!(!source, "failed to copy blobs"))?;
				tracing::trace!(elapsed = ?start.elapsed(), "copy blobs");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let messenger_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_messenger_task(&state, touched_at)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the messenger")
					})?;
				tracing::trace!(elapsed = ?start.elapsed(), "write objects to messenger");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let store_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_store_task(&state, touched_at)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the store")
					})?;
				tracing::trace!(elapsed = ?start.elapsed(), "write objects to store");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let lockfile_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_create_lockfile_task(&state)
					.await
					.map_err(|source| tg::error!(!source, "failed to create lockfile"))?;
				tracing::trace!(elapsed = ?start.elapsed(), "writing lockfile");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		futures::try_join!(
			cache_future,
			messenger_future,
			store_future,
			lockfile_future
		)?;

		let _state = Arc::into_inner(state).unwrap();

		// Create the output.
		let output = tg::checkin::Output { artifact: root };

		Ok(output)
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<tangram_ignore::Ignorer> {
		let file_names = vec![
			".tangramignore".into(),
			".tgignore".into(),
			".gitignore".into(),
		];
		let global = indoc!(
			"
				.DS_Store
				.git
				.tangram
				tangram.lock
			"
		);
		tangram_ignore::Ignorer::new(file_names, Some(global))
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}

	fn checkin_visit(&self, state: &mut State, path: PathBuf) -> tg::Result<Option<usize>> {
		// Check if the path has been visited.
		if let Some(index) = state.graph.paths.get(&path) {
			return Ok(Some(*index));
		}

		// Get the metadata.
		let metadata = std::fs::symlink_metadata(&path).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the metadata"),
		)?;

		// Skip ignored files.
		if state.ignorer.as_mut().map_or(false, |ignorer| {
			ignorer
				.matches(&path, Some(metadata.is_dir()))
				.unwrap_or_default()
		}) {
			return Ok(None);
		}

		// Create the variant.
		let variant = if metadata.is_dir() {
			Variant::Directory(Directory {
				entries: Vec::new(),
			})
		} else if metadata.is_file() {
			Variant::File(File {
				blob: None,
				dependencies: Vec::new(),
				executable: metadata.permissions().mode() & 0o111 != 0,
			})
		} else if metadata.is_symlink() {
			let target = std::fs::read_link(&path)
				.map_err(|source| tg::error!(!source, "failed to read the symlink"))?;
			Variant::Symlink(Symlink { target })
		} else {
			return Err(tg::error!(?metadata, "invalid file type"));
		};

		// If this is a destructive checkin, update permissions/times.
		if state.arg.destructive {
			set_permissions_and_times(&path, &metadata)?;
		}

		// Get the node index.
		let index = state.graph.nodes.len();

		// Update the path.
		state.graph.paths.insert(path.clone(), index);

		// Create the node.
		let node = Node {
			variant,
			metadata: Some(metadata),
			object: None,
			path: Some(Arc::new(path)),
			root: None,
			tag: None,
		};
		state.graph.nodes.push(node);

		// Visit the edges.
		match &state.graph.nodes[index].variant {
			Variant::Directory(_) => self.checkin_visit_directory_edges(state, index)?,
			Variant::File(_) => self.checkin_visit_file_edges(state, index)?,
			Variant::Symlink(_) => Self::checkin_visit_symlink_edges(state, index)?,
		}

		Ok(Some(index))
	}

	fn checkin_visit_directory_edges(&self, state: &mut State, index: usize) -> tg::Result<()> {
		// Read the entries.
		let read_dir = std::fs::read_dir(&state.graph.nodes[index].path())
			.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
		let mut names = Vec::new();
		for result in read_dir {
			let entry = result
				.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?;
			let name = entry
				.file_name()
				.to_str()
				.ok_or_else(|| tg::error!("expected the entry name to be a string"))?
				.to_owned();
			names.push(name);
		}

		// Visit the children.
		for name in names {
			let path = state.graph.nodes[index].path().join(&name);
			let Some(child_index) = self.checkin_visit(state, path)? else {
				continue;
			};
			state.graph.nodes[index]
				.variant
				.unwrap_directory_mut()
				.entries
				.push((name, child_index));
		}

		Ok(())
	}

	fn checkin_visit_file_edges(&self, state: &mut State, index: usize) -> tg::Result<()> {
		// Get the list of all dependencies.
		let path = state.graph.nodes[index].path().to_owned();
		let mut dependencies = self.get_file_dependencies(state, &path)?;

		// Visit path dependencies.
		for dependency in &mut dependencies {
			match dependency {
				FileDependency::Import { import, node, .. }
					if import.reference.path().is_some() =>
				{
					let path = path
						.parent()
						.unwrap()
						.join(import.reference.path().unwrap());
					let path = crate::util::fs::canonicalize_parent_sync(&path).map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to canonicalize path"),
					)?;
					let Some(index) = self.checkin_visit(state, path)? else {
						continue;
					};
					*node = Some(index);
				},
				_ => (),
			}
		}

		// Update the graph.
		state.graph.nodes[index]
			.variant
			.unwrap_file_mut()
			.dependencies = dependencies;

		Ok(())
	}

	fn get_file_dependencies(
		&self,
		state: &mut State,
		path: &Path,
	) -> tg::Result<Vec<FileDependency>> {
		// Check if this file has dependencies set in the xattr.
		if let Ok(Some(contents)) = xattr::get(path, tg::file::XATTR_LOCK_NAME) {
			let lockfile = serde_json::from_slice::<tg::Lockfile>(&contents)
				.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))?;
			if lockfile.nodes.len() != 1 {
				return Err(tg::error!(%path = path.display(), "expected single node in lockfile"));
			}
			let Some(tg::lockfile::Node::File(file_node)) = lockfile.nodes.first() else {
				return Err(tg::error!(%path = path.display(), "expected a file node"));
			};
			return file_node.dependencies.iter().try_fold(
				Vec::new(),
				|mut acc, (reference, referent)| match &referent.item {
					Either::Left(_) => Err(tg::error!("found a graph node")),
					Either::Right(object) => {
						let referent = tg::Referent {
							item: object.clone(),
							path: referent.path.clone(),
							subpath: referent.subpath.clone(),
							tag: referent.tag.clone(),
						};
						let dependency = FileDependency::Referent {
							reference: reference.clone(),
							referent,
						};
						acc.push(dependency);
						Ok(acc)
					},
				},
			);
		}

		// If this is not a module, it has no dependencies.
		if !tg::package::is_module_path(path) {
			return Ok(Vec::new());
		}

		// Parse imports.
		let contents = std::fs::read(path).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to read module file"),
		)?;
		let text = String::from_utf8(contents)
			.map_err(|source| tg::error!(!source, %path = path.display(), "invalid utf8"))?;
		let analysis = crate::Compiler::analyze_module(text)?;
		for error in analysis.errors {
			// Dump diagnostics.
			let diagnostic = tg::Diagnostic {
				location: None,
				message: error.to_string(),
				severity: tg::diagnostic::Severity::Error,
			};
			state.progress.diagnostic(diagnostic);
		}

		// Get the locked dependencies.
		let locked_dependencies = state
			.lockfile
			.as_ref()
			.and_then(|lockfile| lockfile.get_file_dependencies(path).ok())
			.unwrap_or_default();

		// Make sure that the lockfile dependencies matches the import set.
		if state.arg.locked
			&& (locked_dependencies.len() != analysis.imports.len()
				|| !locked_dependencies.iter().all(|(reference, _)| {
					analysis
						.imports
						.iter()
						.any(|import| &import.reference == reference)
				})) {
			return Err(tg::error!(
				"the lockfile needs to be updated but --locked was passed"
			));
		}

		let dependencies =
			analysis
				.imports
				.into_iter()
				.try_fold(Vec::new(), |mut acc, import| {
					// Use the locked dependency if the reference matches.
					if let Some((reference, referent)) =
						locked_dependencies
							.iter()
							.find_map(|(reference, referent)| {
								if &import.reference != reference {
									return None;
								}
								let item = referent.item.as_ref()?.as_ref().right()?.clone();
								let referent = tg::Referent {
									item,
									path: referent.path.clone(),
									subpath: referent.subpath.clone(),
									tag: referent.tag.clone(),
								};
								Some((reference.clone(), referent))
							}) {
						acc.push(FileDependency::Referent {
							reference,
							referent,
						});
						return Ok(acc);
					}

					// Return an error if this dependency can't be resolved.
					if state.locked
						&& import.reference.path().is_none()
						&& import.reference.item().try_unwrap_tag_ref().is_ok()
					{
						return Err(
							tg::error!(%import = import.reference, "unresolved import when --locked"),
						);
					}

					// Compute the path of this import, relative to the root.
					let path = import.reference.path().and_then(|reference| {
						let reference = path.parent().unwrap().join(reference);
						let canonicalized =
							crate::util::fs::canonicalize_parent_sync(reference).ok()?;

						// Diff from the root.
						let diff = crate::util::path::diff(&state.arg.path, &canonicalized).ok()?;

						// Ignore paths that match their imports.
						if Some(diff.as_ref()) == import.reference.path() {
							return None;
						}

						// Ignore empty paths.
						(!diff.as_os_str().is_empty()).then_some(diff)
					});

					//
					let subpath = import
						.reference
						.options()
						.and_then(|opt| opt.subpath.clone());

					// Add the import.
					acc.push(FileDependency::Import {
						import,
						node: None,
						path,
						subpath,
					});

					Ok(acc)
				})?;

		Ok(dependencies)
	}

	fn checkin_visit_symlink_edges(_state: &mut State, _index: usize) -> tg::Result<()> {
		Ok(())
	}

	fn checkin_find_roots(state: &mut State) {
		let mut visited = vec![false; state.graph.nodes.len()];
		let mut stack = vec![(0, None::<usize>)];
		'outer: while let Some((index, hint)) = stack.pop() {
			// Skip nodes that have already been visited.
			if visited[index] {
				continue;
			}
			visited[index] = true;

			// Walk up the path hierarchy.
			let path = state.graph.nodes[index].path();

			// Check if the hint matches.
			if let (Some(hint), false) = (hint, state.graph.nodes[index].is_package()) {
				let hint_path = state.graph.nodes[hint].path();
				if path.strip_prefix(hint_path).is_ok() {
					// Mark this node's root.
					state.graph.nodes[index].root.replace(hint);
					state.graph.roots.entry(hint).or_default().push(index);

					// Recurse on children using the same hint.
					let children = state.graph.nodes[index]
						.edges()
						.into_iter()
						.map(|child| (child, Some(hint)));
					stack.extend(children);
					continue 'outer;
				}
			}

			// Search the ancestors of this node to find its root, unless it is a package.
			let mut root = None;
			if !state.graph.nodes[index].is_package() {
				for ancestor in path.ancestors().skip(1) {
					let Some(node) = state.graph.paths.get(ancestor) else {
						break;
					};
					root.replace(*node);
				}
			}

			// Add to the list of roots if necessary.
			if let Some(root) = root {
				state.graph.roots.entry(root).or_default().push(index);
				state.graph.nodes[index].root.replace(root);
			} else {
				state.graph.roots.entry(index).or_insert(Vec::new());
			}

			// Recurse, using the root if it was discovered or this node if it is a directory.
			let hint = Some(root.unwrap_or(index));
			let children = state.graph.nodes[index]
				.edges()
				.into_iter()
				.map(|child| (child, hint));

			stack.extend(children);
		}
	}

	fn checkin_find_subpaths(state: &mut State) {
		for index in 0..state.graph.nodes.len() {
			if !(state.graph.nodes[index]
				.metadata
				.as_ref()
				.unwrap()
				.is_file() && tg::package::is_module_path(state.graph.nodes[index].path()))
			{
				continue;
			}

			let updates = state.graph.nodes[index]
				.variant
				.unwrap_file_ref()
				.dependencies
				.iter()
				.enumerate()
				.filter_map(|(dep_index, dependency)| {
					// Skip unresolved dependencies.
					let FileDependency::Import {
						import,
						node: Some(node),
						subpath: None,
						..
					} = dependency
					else {
						return None;
					};

					// Skip any imports that have subpaths.
					if import
						.reference
						.options()
						.map_or(false, |options| options.subpath.is_some())
					{
						return None;
					}

					// Get the package and subpath of the import.
					let (package, subpath) = state.graph.nodes[*node].root.map_or_else(
						|| {
							// If the import kind is _not_ a module, make sure to ignore the result.
							if !matches!(import.kind, None | Some(
								tg::module::Kind::Dts |
								tg::module::Kind::Js |
								tg::module::Kind::Ts
							)) {
								return (*node, None);
							}

							// If this is a root directory and contains a tangram.ts, use it.
							let subpath = state.graph.nodes[*node]
								.variant
								.try_unwrap_directory_ref()
								.ok()
								.and_then(|directory| {
									directory.entries.iter().find_map(|(name, _)| {
										tg::package::ROOT_MODULE_FILE_NAMES
											.contains(&name.as_str())
											.then_some(name.into())
									})
								});
							(*node, subpath)
						},
						|root| {
							// Otherwise look for this as a subpath of the module.
							let subpath = state.graph.nodes[*node]
								.path()
								.strip_prefix(state.graph.nodes[root].path())
								.unwrap()
								.to_owned();
							(root, Some(subpath))
						},
					);

					// Use the relative path of the package
					let path = state
						.graph
						.nodes[package]
						.path
						.as_deref()
						.and_then(|module_path| {
							crate::util::path::diff(&state.arg.path, module_path)
								.ok()
						});

					// Recreate the new dependency.
					Some((
						dep_index,
						FileDependency::Import {
							import: import.clone(),
							node: Some(package),
							path,
							subpath,
						},
					))
				})
				.collect::<Vec<_>>(); // iterator ivnalidation.

			// Replace the import with the changed one.
			for (dep_index, dependency) in updates {
				state.graph.nodes[index]
					.variant
					.unwrap_file_mut()
					.dependencies[dep_index] = dependency;
			}
		}
	}

	async fn checkin_create_blobs(&self, state: &mut State) -> tg::Result<()> {
		let blobs = state
			.graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, node)| {
				node.variant.try_unwrap_file_ref().ok().map(|_| {
					tokio::spawn({
						let server = self.clone();
						let path = node.path.as_ref().unwrap().clone();
						async move {
							let _permit = server.file_descriptor_semaphore.acquire().await;
							let mut file = tokio::fs::File::open(path.as_ref()).await.map_err(
								|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
							)?;
							let blob = server.create_blob_inner(&mut file, None).await.map_err(
								|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
							)?;
							Ok::<_, tg::Error>((index, blob))
						}
					})
					.map(|result| result.unwrap())
				})
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		for (index, blob) in blobs {
			state.graph.nodes[index]
				.variant
				.unwrap_file_mut()
				.blob
				.replace(blob);
		}
		Ok(())
	}

	fn checkin_create_objects(state: &mut State) -> tg::Result<()> {
		// Separate into sccs.
		let sccs = petgraph::algo::tarjan_scc(&state.graph);
		for mut scc in sccs {
			// Special case: no cycles.
			if scc.len() == 1 {
				Self::create_normal_object(state, scc[0])?;
				continue;
			}
			scc.reverse();

			let graph_indices = scc
				.iter()
				.copied()
				.enumerate()
				.map(|(local, global)| (global, local))
				.collect::<BTreeMap<_, _>>();

			// Create the graph object.
			let mut graph = tg::graph::Data {
				nodes: Vec::with_capacity(scc.len()),
			};
			for index in &scc {
				Self::add_graph_node(state, &graph_indices, &mut graph, *index)?;
			}

			// Create the graph obejct.
			let data = graph.serialize().unwrap();
			let id = tg::graph::Id::new(&data);
			let object = GraphObject {
				id,
				data: graph,
				bytes: data,
			};

			// Create the objects.
			for (local, global) in scc.iter().copied().enumerate() {
				let (kind, data) = match &state.graph.nodes[global].variant {
					Variant::Directory(_) => {
						let kind = tg::object::Kind::Directory;
						let data = tg::directory::data::Directory::Graph {
							graph: object.id.clone(),
							node: local,
						};
						let data = tg::object::Data::from(data);
						(kind, data)
					},
					Variant::File(_) => {
						let kind = tg::object::Kind::File;
						let data = tg::file::data::File::Graph {
							graph: object.id.clone(),
							node: local,
						};
						let data = tg::object::Data::from(data);
						(kind, data)
					},
					Variant::Symlink(_) => {
						let kind = tg::object::Kind::Symlink;
						let data = tg::symlink::data::Symlink::Graph {
							graph: object.id.clone(),
							node: local,
						};
						let data = tg::object::Data::from(data);
						(kind, data)
					},
				};

				// Create the object.
				let bytes = data
					.serialize()
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				let id = tg::object::Id::new(kind, &bytes);
				let object = Object { bytes, data, id };
				state.graph.nodes[global].object = Some(object);
			}

			// Store the graph.
			state.graph_objects.push(object);
		}
		Ok(())
	}

	fn add_graph_node(
		state: &mut State,
		graph_indices: &BTreeMap<usize, usize>,
		graph: &mut tg::graph::Data,
		index: usize,
	) -> tg::Result<()> {
		let node = match &state.graph.nodes[index].variant {
			Variant::Directory(directory) => {
				let entries = directory
					.entries
					.iter()
					.map(|(name, index)| {
						let name = name.clone();
						if let Some(id) = state.graph.nodes[*index]
							.object
							.as_ref()
							.map(|object| object.id.clone())
						{
							(name, Either::Right(id.try_into().unwrap()))
						} else {
							let index = *graph_indices.get(index).unwrap();
							(name, Either::Left(index))
						}
					})
					.collect();
				let data = tg::graph::data::Directory { entries };
				tg::graph::data::Node::Directory(data)
			},
			Variant::File(file) => {
				let contents = file.blob.as_ref().unwrap().id.clone();
				let dependencies = file
					.dependencies
					.iter()
					.map(|dependency| match dependency {
						FileDependency::Import {
							import,
							node,
							path,
							subpath,
						} => {
							let index = node.clone().ok_or_else(
								|| tg::error!(%import = import.reference, "unresolved import"),
							)?;
							let item = if let Some(id) = state.graph.nodes[index]
								.object
								.as_ref()
								.map(|object| object.id.clone())
							{
								Either::Right(id)
							} else {
								let index = *graph_indices.get(&index).unwrap();
								Either::Left(index)
							};
							let path = path.clone();
							let subpath = subpath.clone();
							let tag = state.graph.nodes[index].tag.clone();
							let referent = tg::Referent {
								item,
								path,
								subpath,
								tag,
							};
							Ok::<_, tg::Error>((import.reference.clone(), referent))
						},
						FileDependency::Referent {
							reference,
							referent,
						} => {
							let referent = tg::Referent {
								item: Either::Right(referent.item.clone()),
								path: referent.path.clone(),
								subpath: referent.subpath.clone(),
								tag: referent.tag.clone(),
							};
							Ok((reference.clone(), referent))
						},
					})
					.try_collect()?;
				let executable = file.executable;
				let data = tg::graph::data::File {
					contents,
					dependencies,
					executable,
				};
				tg::graph::data::Node::File(data)
			},
			Variant::Symlink(symlink) => {
				let target = symlink.target.clone();
				let data = tg::graph::data::Symlink::Target { target };
				tg::graph::data::Node::Symlink(data)
			},
		};
		graph.nodes.push(node);
		Ok(())
	}

	fn create_normal_object(state: &mut State, index: usize) -> tg::Result<()> {
		// Create an object for the node.
		let (kind, data) = match &state.graph.nodes[index].variant {
			Variant::Directory(directory) => {
				let kind = tg::object::Kind::Directory;
				let entries = directory
					.entries
					.iter()
					.map(|(name, index)| {
						let name = name.clone();
						let id = state.graph.nodes[*index]
							.object
							.as_ref()
							.unwrap()
							.id
							.clone()
							.try_into()
							.unwrap();
						(name, id)
					})
					.collect();
				let data = tg::directory::Data::Normal { entries };
				let data = tg::object::Data::from(data);
				(kind, data)
			},
			Variant::File(file) => {
				let kind = tg::object::Kind::File;
				let contents = file.blob.as_ref().unwrap().id.clone();
				let dependencies = file
					.dependencies
					.iter()
					.map(|dependency| match dependency {
						FileDependency::Import {
							import,
							node,
							path,
							subpath,
						} => {
							let index = node.clone().ok_or_else(
								|| tg::error!(%import = import.reference, "unresolved import"),
							)?;
							let item = state.graph.nodes[index].object.as_ref().unwrap().id.clone();
							let path = path.clone();
							let subpath = subpath.clone();
							let tag = state.graph.nodes[index].tag.clone();
							let referent = tg::Referent {
								item,
								path,
								subpath,
								tag,
							};
							Ok::<_, tg::Error>((import.reference.clone(), referent))
						},
						FileDependency::Referent {
							reference,
							referent,
						} => Ok((reference.clone(), referent.clone())),
					})
					.try_collect()?;
				let executable = file.executable;
				let data = tg::file::Data::Normal {
					contents,
					dependencies,
					executable,
				};
				let data = tg::object::Data::from(data);
				(kind, data)
			},
			Variant::Symlink(symlink) => {
				let kind = tg::object::Kind::Symlink;
				let target = symlink.target.clone();
				let data = tg::symlink::Data::Target { target };
				let data = tg::object::Data::from(data);
				(kind, data)
			},
		};

		// Create the object.
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let id = tg::object::Id::new(kind, &bytes);
		let object = Object { bytes, data, id };
		state.graph.nodes[index].object = Some(object);
		Ok(())
	}

	async fn checkin_cache_task(&self, state: Arc<State>, touched_at: i64) -> tg::Result<()> {
		state
			.graph
			.roots
			.iter()
			.map(|(root, nodes)| {
				let server = self.clone();
				let root = *root;
				let nodes = nodes.clone();
				let state = state.clone();
				async move {
					// Copy the root artifact to the cache directory.
					let semaphore = self.file_descriptor_semaphore.acquire().await.unwrap();
					tokio::task::spawn_blocking({
						let server = server.clone();
						let state = state.clone();
						move || server.checkin_copy_or_move_to_cache_directory(&state, root, &nodes)
					})
					.await
					.unwrap()
					.map_err(|source| tg::error!(!source, "failed to copy root object to temp"))?;
					drop(semaphore);

					// Publish a notification that the root is ready to be indexed.
					let id = state.graph.nodes[root]
						.object
						.as_ref()
						.unwrap()
						.id
						.clone()
						.try_into()
						.unwrap();
					let message =
						crate::index::Message::PutCacheEntry(crate::index::PutCacheEntryMessage {
							id,
							touched_at,
						});
					let message = serde_json::to_vec(&message)
						.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
					let _published = server
						.messenger
						.stream_publish("index".to_owned(), message.into())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;
		Ok(())
	}

	fn checkin_copy_or_move_to_cache_directory(
		&self,
		state: &State,
		root: usize,
		nodes: &[usize],
	) -> tg::Result<()> {
		// Extract the root data.
		let root_id = &state.graph.nodes[root].object.as_ref().unwrap().id;
		let root_path = state.graph.nodes[root].path();
		let root_metadata = state.graph.nodes[root].metadata.as_ref().unwrap();
		let root_dst = self.cache_path().join(root_id.to_string());

		// Attempt to rename if this is a destructive checkin.
		if state.arg.destructive {
			match std::fs::rename(root_path, &root_dst) {
				Ok(()) => {
					let epoch =
						filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
					filetime::set_symlink_file_times(&root_dst, epoch, epoch).map_err(
						|source| tg::error!(!source, %path = root_dst.display(), "failed to set the modified time"),
					)?;
					return Ok(());
				},
				Err(ref error) if error.raw_os_error() == Some(libc::EEXIST | libc::ENOTEMPTY) => {
					return Ok(());
				},
				Err(ref error) if error.raw_os_error() == Some(libc::ENOSYS) => (),
				Err(source) => return Err(tg::error!(!source, "failed to rename root")),
			}
		}

		// Otherwise copy to a temp.
		let temp = Temp::new(self);
		if root_metadata.is_file() {
			// Copy the file.
			std::fs::copy(root_path, temp.path())
				.map_err(|source| tg::error!(!source, "failed to copy file"))?;

			// Update permissions.
			set_permissions_and_times(temp.path(), &root_metadata)?;
		} else if root_metadata.is_symlink() {
			// Get the target.
			let target = &state.graph.nodes[root].variant.unwrap_symlink_ref().target;

			// Create the symlink.
			std::os::unix::fs::symlink(target, temp.path())
				.map_err(|source| tg::error!(!source, "failed to create link"))?;
		} else {
			// Walk the subgraph.
			std::fs::create_dir_all(&temp.path())
				.map_err(|source| tg::error!(!source, "failed to create temp directory"))?;

			// Copy everything except the root.
			for node in nodes {
				let node = &state.graph.nodes[*node];
				let metadata = node.metadata.as_ref().unwrap();
				let src = node.path();
				let dst = temp.path().join(src.strip_prefix(root_path).unwrap());
				if metadata.is_dir() {
					std::fs::create_dir_all(&dst)
						.map_err(|source| tg::error!(!source, "failed to create directory"))?;
				} else if metadata.is_file() {
					std::fs::copy(src, &dst)
						.map_err(|source| tg::error!(!source, "failed to copy file"))?;
				} else if metadata.is_symlink() {
					let target = &node.variant.unwrap_symlink_ref().target;
					std::os::unix::fs::symlink(target, &dst)
						.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
				} else {
					unreachable!()
				}

				// Update permissions.
				set_permissions_and_times(&dst, metadata)?;
			}
		}

		// Rename the temp to the cache.
		match std::fs::rename(temp.path(), &root_dst) {
			Ok(()) => (),
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::DirectoryNotEmpty,
				) =>
			{
				()
			},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"failed to rename the path to the cache directory"
				));
			},
		}

		// Set the file times to the epoch.
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(&root_dst, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = root_dst.display(), "failed to set the modified time"),
		)?;

		Ok(())
	}

	async fn checkin_messenger_task(&self, state: &Arc<State>, touched_at: i64) -> tg::Result<()> {
		if self.messenger.is_left() {
			self.checkin_messenger_memory(state, touched_at).await?;
		} else {
			self.checkin_messenger_nats(state, touched_at).await?;
		}
		Ok(())
	}

	async fn checkin_messenger_memory(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut messages: Vec<Bytes> = Vec::new();
		for (root, nodes) in &state.graph.roots {
			let nodes = std::iter::once(*root).chain(nodes.iter().copied());
			let root: tg::artifact::Id = state.graph.nodes[*root]
				.object
				.as_ref()
				.unwrap()
				.id
				.clone()
				.try_into()
				.unwrap();
			for node in nodes {
				let node = &state.graph.nodes[node];
				let object = node.object.as_ref().unwrap();
				let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
					children: object.data.children(),
					id: object.id.clone(),
					size: object.bytes.len().to_u64().unwrap(),
					touched_at,
					cache_reference: None,
				});
				let message = serde_json::to_vec(&message)
					.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
				messages.push(message.into());
				// Send messages for the blob.
				if let Variant::File(File {
					blob: Some(blob), ..
				}) = &node.variant
				{
					let mut stack = vec![blob];
					while let Some(blob) = stack.pop() {
						let children = blob
							.data
							.as_ref()
							.map(tg::blob::Data::children)
							.unwrap_or_default();
						let id = blob.id.clone().into();
						let size = blob.size;
						let message =
							crate::index::Message::PutObject(crate::index::PutObjectMessage {
								cache_reference: Some(root.clone()),
								children,
								id,
								size,
								touched_at,
							});
						let message = serde_json::to_vec(&message).map_err(|source| {
							tg::error!(!source, "failed to serialize the message")
						})?;
						messages.push(message.into());
						stack.extend(&blob.children);
					}
				}
			}
		}
		while !messages.is_empty() {
			let published = self
				.messenger
				.stream_batch_publish("index".to_owned(), messages.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;
			messages = messages.split_off(published.len());
		}
		Ok(())
	}

	async fn checkin_messenger_nats(&self, state: &Arc<State>, touched_at: i64) -> tg::Result<()> {
		for (root, nodes) in &state.graph.roots {
			let nodes = std::iter::once(*root).chain(nodes.iter().copied());
			let root: tg::artifact::Id = state.graph.nodes[*root]
				.object
				.as_ref()
				.unwrap()
				.id
				.clone()
				.try_into()
				.unwrap();
			for node in nodes {
				let node = &state.graph.nodes[node];
				let object = node.object.as_ref().unwrap();
				let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
					children: object.data.children(),
					id: object.id.clone(),
					size: object.bytes.len().to_u64().unwrap(),
					touched_at,
					cache_reference: None,
				});
				let message = serde_json::to_vec(&message)
					.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
				let _published = self
					.messenger
					.stream_publish("index".to_owned(), message.into())
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

				// Send messages for the blob.
				if let Variant::File(File {
					blob: Some(blob), ..
				}) = &node.variant
				{
					let mut stack = vec![blob];
					while let Some(blob) = stack.pop() {
						let children = blob
							.data
							.as_ref()
							.map(tg::blob::Data::children)
							.unwrap_or_default();
						let id = blob.id.clone().into();
						let size = blob.size;
						let message =
							crate::index::Message::PutObject(crate::index::PutObjectMessage {
								cache_reference: Some(root.clone()),
								children,
								id,
								size,
								touched_at,
							});
						let message = serde_json::to_vec(&message).map_err(|source| {
							tg::error!(!source, "failed to serialize the message")
						})?;
						let _published = self
							.messenger
							.stream_publish("index".to_owned(), message.into())
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to publish the message")
							})?;
						stack.extend(&blob.children);
					}
				}
			}
		}
		Ok(())
	}

	async fn checkin_store_task(&self, state: &State, touched_at: i64) -> tg::Result<()> {
		let mut objects = Vec::with_capacity(state.graph.nodes.len());

		// Add the graph objects.
		for graph in &state.graph_objects {
			objects.push((graph.id.clone().into(), Some(graph.bytes.clone()), None));
		}

		// Add the nodes.
		for (root, nodes) in &state.graph.roots {
			let nodes = std::iter::once(*root).chain(nodes.iter().copied());
			let root_path = state.graph.nodes[*root].path();
			let root: tg::artifact::Id = state.graph.nodes[*root]
				.object
				.as_ref()
				.unwrap()
				.id
				.clone()
				.try_into()
				.unwrap();
			for node in nodes {
				let node = &state.graph.nodes[node];

				// Add the object to the list to store.
				let object = node.object.as_ref().unwrap();
				objects.push((object.id.clone(), Some(object.bytes.clone()), None));

				// If the file has a blob, add it too.
				if let Variant::File(File {
					blob: Some(blob), ..
				}) = &node.variant
				{
					let mut stack = vec![blob];
					while let Some(blob) = stack.pop() {
						let subpath = node.path().strip_prefix(root_path).unwrap().to_owned();
						let subpath = subpath
							.to_str()
							.unwrap()
							.is_empty()
							.not()
							.then_some(subpath);
						let reference = crate::store::CacheReference {
							artifact: root.clone(),
							subpath,
							position: blob.position,
							length: blob.length,
						};
						objects.push((blob.id.clone().into(), blob.bytes.clone(), Some(reference)));
						stack.extend(&blob.children);
					}
				}
			}
		}
		let arg = crate::store::PutBatchArg {
			objects,
			touched_at,
		};
		self.store
			.put_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;

		Ok(())
	}

	async fn checkin_create_lockfile_task(&self, state: &State) -> tg::Result<()> {
		// Skip creating a lockfile if this is a destructive checkin, the caller did not request a lockfile, or the root is not a directory.
		if state.arg.destructive
			|| !state.arg.lockfile
			|| !state.graph.nodes[0].metadata.as_ref().unwrap().is_dir()
		{
			return Ok(());
		}

		// Create a lockfile.
		let lockfile = Self::create_lockfile(state)?;

		// Do nothing if the lockfile is empty.
		if lockfile.nodes.is_empty() {
			return Ok(());
		}

		// Serialize the lockfile.
		let contents = serde_json::to_vec_pretty(&lockfile)
			.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;

		// Write to disk.
		tokio::fs::write(
			state.graph.nodes[0]
				.path()
				.join(tg::package::LOCKFILE_FILE_NAME),
			contents,
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
		Ok(())
	}

	pub(crate) async fn ignore_matcher_for_checkin() -> tg::Result<ignore::Matcher> {
		let file_names = vec![
			".tangramignore".into(),
			".tgignore".into(),
			".gitignore".into(),
		];
		let global = indoc!(
			"
				.DS_Store
				.git
				.tangram
				tangram.lock
			"
		);
		ignore::Matcher::new(file_names, Some(global))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}

	pub(crate) async fn handle_checkin_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.checkin(arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl Node {
	fn is_package(&self) -> bool {
		self.variant
			.try_unwrap_directory_ref()
			.map_or(false, |directory| {
				directory
					.entries
					.iter()
					.any(|(name, _)| tg::package::ROOT_MODULE_FILE_NAMES.contains(&name.as_str()))
			})
	}

	fn path(&self) -> &Path {
		self.path.as_deref().unwrap()
	}

	fn edges(&self) -> Vec<usize> {
		match &self.variant {
			Variant::Directory(directory) => {
				directory.entries.iter().map(|(_, node)| *node).collect()
			},
			Variant::File(file) => file
				.dependencies
				.iter()
				.filter_map(|dependency| match dependency {
					FileDependency::Import { node, .. } => *node,
					FileDependency::Referent { .. } => None,
				})
				.collect(),
			Variant::Symlink(_) => Vec::new(),
		}
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl petgraph::visit::NodeIndexable for &Graph {
	fn from_index(&self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(&self) -> usize {
		self.nodes.len()
	}

	fn to_index(&self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a Graph {
	type Neighbors = std::vec::IntoIter<usize>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		self.nodes[a].edges().into_iter()
	}
}

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

fn set_permissions_and_times(path: &Path, metadata: &std::fs::Metadata) -> tg::Result<()> {
	if !metadata.is_symlink() {
		let mode = metadata.permissions().mode();
		let executable = mode & 0o111 != 0;
		let new_mode = if metadata.is_dir() || executable {
			0o755
		} else {
			0o644
		};
		if new_mode != mode {
			let permissions = std::fs::Permissions::from_mode(new_mode);
			std::fs::set_permissions(&path, permissions).map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to set the permissions"),
			)?;
		}
	}
	let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
	filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
	)?;
	Ok(())
}
