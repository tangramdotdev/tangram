use crate::{Server, blob::create::Blob, lockfile::ParsedLockfile};
use bytes::Bytes;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
	stream::FuturesUnordered,
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
	data: Bytes,
}

#[derive(Clone, Debug)]
pub struct Graph {
	nodes: Vec<Node>,
	paths: radix_trie::Trie<PathBuf, usize>,
	roots: Vec<usize>,
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
struct Edge {
	node: Option<usize>,
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

		if arg.destructive {
			self.checkin_new(arg, progress).await
		} else {
			self.checkin_old(arg, progress).await
		}
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
		let lockfile = self.try_parse_lockfile(&root.join(tg::package::LOCKFILE_FILE_NAME))?;

		// Create the state.
		let graph = Graph {
			nodes: Vec::new(),
			paths: radix_trie::Trie::default(),
			roots: Vec::new(),
		};
		let mut state = State {
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

		// Create blobs.
		let start = Instant::now();
		self.checkin_create_blobs(&mut state).await?;
		tracing::trace!(elapsed = ?start.elapsed(), "create blobs");

		// Create objects.
		let start = Instant::now();
		Self::checkin_create_objects(&mut state, 0)?;
		tracing::trace!(elapsed = ?start.elapsed(), "create objects");

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Get the root node's ID.
		let root = state.graph.nodes[0].object.as_ref().unwrap().id.clone();

		// Write the objects to the database and the store.
		let state = Arc::new(state);
		let cache_future = tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_cache_task(state, &arg, touched_at)
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
			let root = root.clone();
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
			let arg = arg.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_store_task(&arg, &state, touched_at)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the store")
					})?;
				tracing::trace!(elapsed = ?start.elapsed(), "write objects to store");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		futures::try_join!(cache_future, messenger_future, store_future)?;
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

	fn checkin_visit(&self, state: &mut State, path: PathBuf) -> tg::Result<usize> {
		// Check if the path has been visited.
		if let Some(index) = state.graph.paths.get(&path) {
			return Ok(*index);
		}

		// Get the metadata.
		let metadata = std::fs::symlink_metadata(&path)
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;

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

		Ok(index)
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
			let index = self.checkin_visit(state, path)?;
			state.graph.nodes[index]
				.variant
				.unwrap_directory_mut()
				.entries
				.push((name, index));
		}

		Ok(())
	}

	fn checkin_visit_file_edges(&self, state: &mut State, index: usize) -> tg::Result<()> {
		// Get the list of all dependencies.
		let path = state.graph.nodes[index].path().to_owned();
		let mut dependencies =
			self.get_file_dependencies(state, &path)?;

		// Visit path dependencies.
		for dependency in &mut dependencies {
			match dependency {
				FileDependency::Import { import, node } if import.reference.path().is_some() => {
					let index =
						self.checkin_visit(state, import.reference.path().unwrap().to_owned())?;
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
		if locked_dependencies.len() != analysis.imports.len()
			|| !locked_dependencies.iter().all(|(reference, _)| {
				analysis
					.imports
					.iter()
					.any(|import| &import.reference == reference)
			}) {
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

					// Add the import.
					acc.push(FileDependency::Import { import, node: None });

					Ok(acc)
				})?;

		Ok(dependencies)
	}

	fn checkin_visit_symlink_edges(
		_state: &mut State,
		_index: usize,
	) -> tg::Result<()> {
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
			if let Some(hint) = hint {
				let hint_path = state.graph.nodes[hint]
					.path();
				if path.strip_prefix(hint_path).is_ok() {
					state.graph.nodes[index].root = Some(hint);

					// Recurse on children using the same hint.
					let children = state.graph.nodes[index].edges()
						.into_iter()
						.map(|child| (child, Some(hint)));
					stack.extend(children);
					continue 'outer;
				}
			}

			// Search the ancestors of this node to find its root.
			let mut root = None;
			for ancestor in path.ancestors().skip(1) {
				let Some(node) = state.graph.paths.get(ancestor) else {
					break;
				};
				root.replace(*node);
			}
			state.graph.nodes[index].root = root;

			// Add to the list of roots if necessary.
			if root.is_none() {
				state.graph.roots.push(index);
			}

			// Recurse, using the root if it was discovered or this node if it is a directory.
			let hint = Some(root.unwrap_or(index));
			let children = state.graph.nodes[index].edges()
				.into_iter()
				.map(|child| (child, hint));

			stack.extend(children);
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

	fn checkin_create_objects(state: &mut State, index: usize) -> tg::Result<()> {
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
			let object = GraphObject { id, data };

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
				state.graph.nodes[index].object = Some(object);
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
				let dependencies = file.dependencies
					.iter()
					.map(|dependency| {
						match dependency {
							FileDependency::Import { import, node } => {
								let index = node.clone().ok_or_else(|| tg::error!(%import = import.reference, "unresolved import"))?;
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
								let path = state
									.graph
									.nodes[index]
									.path
									.as_ref()
									.and_then(|this_path| {
										let root_path = state.graph.nodes.first()?.path.as_deref()?;
										crate::util::path::diff(root_path, this_path).ok()
									});
								let tag = state.graph.nodes[index].tag.clone();
								let referent = tg::Referent {
									item,
									path,
									subpath: None,  // TODO
									tag,
								};
								Ok::<_, tg::Error>((import.reference.clone(), referent))
							},
							FileDependency::Referent { reference, referent } => {
								let referent = tg::Referent {
									item: Either::Right(referent.item.clone()),
									path: referent.path.clone(),
									subpath: referent.subpath.clone(),
									tag: referent.tag.clone()
								};
								Ok((reference.clone(), referent))
							},
						}
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
				let dependencies = file.dependencies
					.iter()
					.map(|dependency| {
						match dependency {
							FileDependency::Import { import, node } => {
								let index = node.clone().ok_or_else(|| tg::error!(%import = import.reference, "unresolved import"))?;
								let item = state
									.graph
									.nodes[index]
									.object
									.as_ref()
									.unwrap()
									.id
									.clone();
								let path = state
								.graph
								.nodes[index]
								.path
								.as_ref()
								.and_then(|this_path| {
									let root_path = state.graph.nodes.first()?.path.as_deref()?;
									crate::util::path::diff(root_path, this_path).ok()
								});
								let tag = state.graph.nodes[index].tag.clone();
								let referent = tg::Referent {
									item,
									path,
									subpath: None,  // TODO
									tag,
								};
								Ok::<_, tg::Error>((import.reference.clone(), referent))
							},
							FileDependency::Referent { reference, referent } => {
								Ok((reference.clone(), referent.clone()))
							}
						}
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

	async fn checkin_cache_task(
		&self,
		state: Arc<State>,
		arg: &tg::checkin::Arg,
	) -> tg::Result<()> {
		if arg.destructive {

		}
		todo!()
	}

	async fn checkin_cache_task_destructive(
		&self,
		state: Arc<State>,
		arg: &tg::checkin::Arg,
		root: &tg::artifact::Id,
		touched_at: i64,
	) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let path = arg.path.clone();
			move || {
				let mut stack = vec![path];
				while let Some(path) = stack.pop() {
					let index = *state.graph.paths.get(&path).unwrap();
					let metadata = state.graph.nodes[index].metadata.as_ref().unwrap();
					let children = state
							.graph
							.nodes[index]
							.edges()
							.into_iter()
							.filter_map(|edge| {
							state
								.graph
								.nodes
								.get(edge)
								.unwrap()
								.path
								.as_deref()
								.cloned()
						});
						stack.extend(children);
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
					let epoch =
						filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
					filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
					)?;
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		// Rename the path to the cache directory.
		let src = &arg.path;
		let dst = self.cache_path().join(root.to_string());
		let result = tokio::fs::rename(src, dst).await;
		match result {
			Ok(()) => (),
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::DirectoryNotEmpty,
				) => {},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"failed to rename the path to the cache directory"
				));
			},
		}

		// Set the file times to the epoch.
		let path = self.cache_path().join(root.to_string());
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
		)?;

		// Send a message for the cache entry.
		let message = crate::index::Message::PutCacheEntry(crate::index::PutCacheEntryMessage {
			id: root.clone(),
			touched_at,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	async fn checkin_messenger_task(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		if self.messenger.is_left() {
			self.checkin_messenger_memory(state, touched_at)
				.await?;
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
		for node in &state.graph.nodes {
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
					let message = serde_json::to_vec(&message)
						.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
					messages.push(message.into());
					stack.extend(&blob.children);
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

	async fn checkin_messenger_nats(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {

		for node in &state.graph.nodes {
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
					let message = serde_json::to_vec(&message)
						.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
					let _published = self
						.messenger
						.stream_publish("index".to_owned(), message.into())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
					stack.extend(&blob.children);
				}
			}
		}
		Ok(())
	}

	async fn checkin_store_task(
		&self,
		arg: &tg::checkin::Arg,
		state: &State,
		root: &tg::artifact::Id,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut objects = Vec::with_capacity(state.graph.nodes.len());
		for node in &state.graph.nodes {
			let object = node.object.as_ref().unwrap();
			objects.push((object.id.clone(), Some(object.bytes.clone()), None));
			if let Variant::File(File {
				blob: Some(blob), ..
			}) = &node.variant
			{
				let mut stack = vec![blob];
				while let Some(blob) = stack.pop() {
					let subpath = node
						.path
						.as_ref()
						.unwrap()
						.strip_prefix(&arg.path)
						.unwrap()
						.to_owned();
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
			} else {
				objects.push((object.id.clone(), Some(object.bytes.clone()), None));
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

	// Check in the artifact.
	async fn checkin_old(
		&self,
		arg: tg::checkin::Arg,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::checkin::Output> {
		// Create the input graph.
		progress.spinner("input", "collecting input...");
		progress.start(
			"files".into(),
			"files".into(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		let input_graph = self
			.create_input_graph(arg.clone(), progress)
			.await
			.map_err(
				|source| tg::error!(!source, %path = arg.path.display(), "failed to collect the input"),
			)?;
		progress.finish("input");

		// Create the unification graph and get its root node.
		progress.spinner("unify", "unifying...");
		let (unification_graph, root) = self
			.create_unification_graph(&input_graph, arg.deterministic)
			.await
			.map_err(|source| tg::error!(!source, "failed to unify dependencies"))?;
		progress.finish("unify");

		// Create the object graph.
		progress.spinner("create-objects", "creating objects...");
		progress.start(
			"objects".into(),
			"objects".into(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		let object_graph = self
			.create_object_graph(&input_graph, &unification_graph, &root, progress)
			.await
			.map_err(|source| tg::error!(!source, "failed to create objects"))?;
		progress.finish("create-objects");
		progress.finish("objects");

		// Create the output graph.
		progress.spinner("output", "collecting output...");
		let output_graph = self
			.create_output_graph(&input_graph, &object_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to write objects"))?;
		progress.finish("output");

		// Cache the files.
		progress.spinner("cache", "caching objects...");
		self.checkin_cache_task_old(&output_graph, &input_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to copy the blobs"))?;
		progress.finish("cache");

		// Write the output to the database and the store.
		progress.spinner("output", "writing output...");
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		futures::try_join!(
			self.write_output_to_messenger(output_graph.clone(), object_graph.clone(), touched_at)
				.map_err(|source| tg::error!(!source, "failed to store the objects")),
			self.write_output_to_store(output_graph.clone(), object_graph.clone(), touched_at)
				.map_err(|source| tg::error!(!source, "failed to store the objects"))
		)?;
		progress.finish("output");

		// Copy or move to the cache directory.
		if arg.cache || arg.destructive {
			self.copy_or_move_to_cache_directory(&input_graph, &output_graph, 0, progress)
				.await
				.map_err(|source| tg::error!(!source, "failed to cache the artifact"))?;
		}

		// Get the artifact.
		let artifact = output_graph.nodes[0].id.clone();

		// If this is a non-destructive checkin, then attempt to create a lockfile.
		if arg.lockfile && !arg.destructive && artifact.is_directory() {
			progress.spinner("lockfile", "creating lockfile...");
			self.try_write_lockfile(&input_graph, &object_graph)
				.await
				.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
			progress.finish("lockfile");
		}

		// Create the output.
		let output = tg::checkin::Output { artifact };

		Ok(output)
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
