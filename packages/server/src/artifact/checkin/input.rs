use crate::Server;
use futures::{
	stream::{self, FuturesUnordered},
	TryStreamExt,
};
use std::{
	collections::{BTreeMap, BTreeSet, VecDeque},
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_either::Either;

use crate::util::module::{is_module_path, is_root_module_path, try_get_root_module_path_for_path};

#[derive(Clone, Debug)]
pub struct Graph {
	pub arg: tg::artifact::checkin::Arg,
	pub dependencies: Option<BTreeMap<tg::Reference, Dependency>>,
	pub is_root: bool,
	pub is_direct_dependency: bool,
	pub metadata: std::fs::Metadata,
	pub lockfile: Option<(Arc<tg::Lockfile>, tg::Path)>,
	pub parent: Option<Weak<RwLock<Self>>>,
}

pub type Dependency = Option<Either<tg::object::Id, Arc<RwLock<Graph>>>>;

struct State {
	roots: Vec<tg::Path>,
	visited: BTreeMap<tg::Path, Arc<RwLock<Graph>>>,
}

impl Server {
	pub(super) async fn collect_input(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: &super::ProgressState,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		let state = RwLock::new(State {
			roots: Vec::new(),
			visited: BTreeMap::new(),
		});
		let input = self
			.collect_input_inner(arg, 0, None, false, &state, progress, None)
			.await?;
		if input.read().unwrap().metadata.is_dir() {
			self.reparent_file_path_dependencies(input.clone()).await?;
		}
		Ok(input)
	}

	#[allow(clippy::too_many_arguments)]
	async fn collect_input_inner(
		&self,
		arg: tg::artifact::checkin::Arg,
		root: usize,
		lockfile: Option<(Arc<tg::Lockfile>, tg::Path)>,
		is_direct_dependency: bool,
		state: &RwLock<State>,
		progress: &super::ProgressState,
		parent: Option<Weak<RwLock<Graph>>>,
	) -> tg::Result<Arc<RwLock<Graph>>> {
		if let Some(visited) = state.read().unwrap().visited.get(&arg.path).cloned() {
			return Ok(visited);
		}

		// Detect if this is a root or not.
		let is_root = state.read().unwrap().roots.iter().all(|root| {
			let first_component = root
				.diff(&arg.path)
				.and_then(|p| p.components().first().cloned());
			!matches!(first_component, Some(tg::path::Component::Parent))
		});

		// Add to the roots if necessary.
		let root = if is_root {
			let mut state_ = state.write().unwrap();
			let root = state_.roots.len();
			state_.roots.push(arg.path.clone());
			root
		} else {
			root
		};

		// Get the file system metatadata.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let metadata = tokio::fs::symlink_metadata(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path, "failed to get file metadata"),
		)?;
		drop(permit);

		// Check if there's a lockfile.
		let lockfile = 'a: {
			// Try and re-use the parent if possible.
			if let Some((lockfile, root)) = lockfile.clone() {
				break 'a Some((lockfile, root));
			}

			// Otherwise try and read locally.
			if let Some((lockfile, root)) = self.try_read_lockfile(&arg.path, &metadata).await? {
				break 'a Some((Arc::new(lockfile), root));
			}

			if is_root_module_path(arg.path.as_ref()) {
				break 'a Some((Arc::new(tg::Lockfile::default()), arg.path.clone()));
			}

			None
		};

		// Create the input, without its dependencies.
		let input = Arc::new(RwLock::new(Graph {
			arg: arg.clone(),
			dependencies: None,
			is_root,
			is_direct_dependency,
			lockfile: lockfile.clone(),
			metadata: metadata.clone(),
			parent,
		}));

		// Update state.
		state
			.write()
			.unwrap()
			.visited
			.insert(arg.path.clone(), input.clone());

		// Get the dependencies.
		if let Some(dependencies) = self
			.get_dependencies(&arg.path, &metadata)
			.await
			.map_err(|source| tg::error!(!source, %path = arg.path, "failed to get dependencies"))?
		{
			// Recurse.
			let dependencies = dependencies
				.into_iter()
				.map(|(reference, object)| async {
					let Some(path) = reference
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| reference.query()?.path.as_ref())
					else {
						return Ok((reference, None));
					};
					if let Some(object) = object {
						return Ok((reference, Some(Either::Left(object))));
					}
					let parent_path = if metadata.is_dir() {
						arg.path.clone()
					} else {
						arg.path.clone().parent()
					};
					let is_direct_dependency = is_module_path(arg.path.as_ref());
					let arg = tg::artifact::checkin::Arg {
						path: parent_path.join(path.clone()).normalize(),
						..arg.clone()
					};
					let child = Box::pin(self.collect_input_inner(
						arg,
						root,
						lockfile.clone(),
						is_direct_dependency,
						state,
						progress,
						Some(Arc::downgrade(&input)),
					))
					.await?;
					Ok::<_, tg::Error>((reference, Some(Either::Right(child))))
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;
			input.write().unwrap().dependencies = Some(dependencies);
		}

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

	async fn get_dependencies(
		&self,
		path: &tg::Path,
		metadata: &std::fs::Metadata,
	) -> tg::Result<Option<Vec<(tg::Reference, Option<tg::object::Id>)>>> {
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		if metadata.is_dir() {
			if let Some(root_module_path) = try_get_root_module_path_for_path(path.as_ref()).await?
			{
				return Ok(Some(vec![(
					tg::Reference::with_path(&root_module_path),
					None,
				)]));
			}
			self.get_directory_dependencies(path).await.map(Some)
		} else if metadata.is_file() {
			self.get_file_dependencies(path).await
		} else if metadata.is_symlink() {
			Ok(None)
		} else {
			Err(tg::error!(%path, "invalid file type"))
		}
	}

	async fn get_file_dependencies(
		&self,
		path: &tg::Path,
	) -> tg::Result<Option<Vec<(tg::Reference, Option<tg::object::Id>)>>> {
		if let Some(data) = xattr::get(path, tg::file::XATTR_NAME)
			.map_err(|source| tg::error!(!source, "failed to read file xattr"))?
		{
			let dependencies: Either<Vec<tg::object::Id>, BTreeMap<tg::Reference, tg::object::Id>> =
				serde_json::from_slice(&data)
					.map_err(|source| tg::error!(!source, "failed to deserialize xattr"))?;
			let dependencies = match dependencies {
				Either::Left(set) => set
					.into_iter()
					.map(|v| (tg::Reference::with_object(&v), None))
					.collect(),
				Either::Right(map) => map
					.into_iter()
					.map(|(reference, id)| {
						let id = reference
							.path()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.query()?.path.as_ref())
							.is_none()
							.then_some(id);
						(reference, id)
					})
					.collect(),
			};
			return Ok(Some(dependencies));
		}

		if is_module_path(path.as_ref()) || is_root_module_path(path.as_ref()) {
			return Ok(Some(self.get_module_dependencies(path).await?));
		}

		Ok(None)
	}

	async fn get_module_dependencies(
		&self,
		path: &tg::Path,
	) -> tg::Result<Vec<(tg::Reference, Option<tg::object::Id>)>> {
		let text = tokio::fs::read_to_string(path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to read file contents"))?;
		let analysis = crate::compiler::Compiler::analyze_module(text)
			.map_err(|source| tg::error!(!source, "failed to analyze module"))?;
		let dependencies = analysis
			.imports
			.into_iter()
			.map(|import| (import.reference, None))
			.collect();
		Ok(dependencies)
	}

	async fn get_directory_dependencies(
		&self,
		path: &tg::Path,
	) -> tg::Result<Vec<(tg::Reference, Option<tg::object::Id>)>> {
		let entries = tokio::fs::read_dir(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to get directory entries"))?;
		stream::try_unfold(entries, move |mut entries| async {
			let Some(entry) = entries
				.next_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to get next directory entry"))?
			else {
				return Ok(None);
			};
			let path = entry
				.file_name()
				.to_str()
				.ok_or_else(|| tg::error!("invalid file name"))?
				.parse()?;
			let reference = tg::Reference::with_path(&path);
			Ok::<_, tg::Error>(Some(((reference, None), entries)))
		})
		.try_collect()
		.await
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
			let Some(dependencies) = input.read().unwrap().dependencies.clone() else {
				continue;
			};

			// If this input is a file, reparent its children.
			if input.read().unwrap().metadata.is_file() {
				let dependencies = dependencies.iter().filter_map(|(reference, child)| {
					let child = child.as_ref()?.as_ref().right()?.clone();
					reference
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| reference.query()?.path.as_ref())
						.is_some()
						.then_some(child)
				});
				for child in dependencies {
					self.reparent_file_path_dependency(child).await?;
				}
			}

			// Add its children to the stack.
			let children = dependencies
				.values()
				.filter_map(|either| either.as_ref()?.as_ref().right())
				.cloned();
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
		let mut components: VecDeque<_> = child
			.read()
			.unwrap()
			.arg
			.path
			.diff(&parent.read().unwrap().arg.path)
			.unwrap()
			.into_components()
			.into();

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
					parent
						.write()
						.unwrap()
						.dependencies
						.as_mut()
						.unwrap()
						.insert(reference, Some(Either::Right(child.clone())));
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
						.dependencies
						.as_ref()
						.unwrap()
						.get(&reference)
						.and_then(|inner| inner.as_ref()?.as_ref().right().cloned());
					if let Some(input) = input {
						parent = input;
						continue;
					}
					let arg = parent.read().unwrap().arg.clone();
					let path = arg.path.clone().join(&name);
					let metadata = tokio::fs::metadata(&path).await.map_err(|source| {
						tg::error!(!source, "failed to get directory metadata")
					})?;
					let arg = tg::artifact::checkin::Arg { path, ..arg };
					let child = Arc::new(RwLock::new(Graph {
						arg,
						dependencies: Some(BTreeMap::new()),
						is_root: false,
						is_direct_dependency: false,
						metadata,
						lockfile: parent.read().unwrap().lockfile.clone(),
						parent: Some(Arc::downgrade(&parent)),
					}));
					parent
						.write()
						.unwrap()
						.dependencies
						.as_mut()
						.unwrap()
						.insert(reference, Some(Either::Right(child.clone())));
					parent = child;
				},
				_ => return Err(tg::error!("unepxected path component")),
			}
		}

		Err(tg::error!("failed to reparent orphaned input node"))
	}
}
