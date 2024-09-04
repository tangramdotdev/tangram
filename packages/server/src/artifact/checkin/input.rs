use crate::Server;
use futures::{
	stream::{self, FuturesUnordered},
	TryStreamExt,
};
use std::{
	collections::BTreeMap,
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_either::Either;

#[derive(Clone, Debug)]
pub struct Input {
	pub arg: tg::artifact::checkin::Arg,
	pub dependencies: Option<BTreeMap<tg::Reference, Dependency>>,
	pub is_root: bool,
	pub is_direct_dependency: bool,
	pub metadata: std::fs::Metadata,
	pub lockfile: Option<(Arc<tg::Lockfile>, tg::Path)>,
	pub parent: Option<Weak<RwLock<Self>>>,
}

pub type Dependency = Option<Either<tg::object::Id, Arc<RwLock<Input>>>>;

struct State {
	roots: Vec<tg::Path>,
	visited: BTreeMap<tg::Path, Arc<RwLock<Input>>>,
}

impl Server {
	pub(super) async fn collect_input(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: &super::ProgressState,
	) -> tg::Result<Arc<RwLock<Input>>> {
		let state = RwLock::new(State {
			roots: Vec::new(),
			visited: BTreeMap::new(),
		});
		self.collect_input_inner(arg, 0, None, false, &state, progress, None)
			.await
	}

	async fn collect_input_inner(
		&self,
		arg: tg::artifact::checkin::Arg,
		root: usize,
		lockfile: Option<(Arc<tg::Lockfile>, tg::Path)>,
		is_direct_dependency: bool,
		state: &RwLock<State>,
		progress: &super::ProgressState,
		parent: Option<Weak<RwLock<Input>>>,
	) -> tg::Result<Arc<RwLock<Input>>> {
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

			if tg::module::is_root_module_path(arg.path.as_ref()) {
				break 'a Some((Arc::new(tg::Lockfile::default()), arg.path.clone()));
			}

			None
		};

		// Create the input, without its dependencies.
		let input = Arc::new(RwLock::new(Input {
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
					let is_direct_dependency = tg::module::is_module_path(arg.path.as_ref());
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
		let mut root = if metadata.is_file() && tg::module::is_root_module_path(path.as_ref()) {
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
			if let Some(root_module_path) =
				tg::module::try_get_root_module_path_for_path(path.as_ref()).await?
			{
				let module_path = path.clone().join(root_module_path.clone()).normalize();
				let module_dependencies = self
					.get_module_dependencies(&module_path)
					.await?
					.into_iter()
					.filter(|(reference, _)| {
						let Some(path) = reference
							.path()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.query()?.path.as_ref())
						else {
							return false;
						};
						matches!(
							path.components().first(),
							Some(tg::path::Component::Current)
						)
					})
					.chain(std::iter::once((
						tg::Reference::with_path(&root_module_path),
						None,
					)))
					.collect();
				return Ok(Some(module_dependencies));
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
				Either::Right(map) => map.into_iter().map(|(k, v)| (k, Some(v))).collect(),
			};
			return Ok(Some(dependencies));
		}

		if tg::module::is_module_path(path.as_ref())
			|| tg::module::is_root_module_path(path.as_ref())
		{
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
}
