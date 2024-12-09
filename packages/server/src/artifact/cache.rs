use super::Progress;
use crate::{temp::Temp, Server};
use futures::{stream::FuturesUnordered, Future, FutureExt as _, TryStreamExt as _};
use num::ToPrimitive as _;
use std::{os::unix::fs::PermissionsExt as _, path::PathBuf, sync::Arc};
use tangram_client::{self as tg, handle::Ext, object::Metadata};
use tokio_util::io::InspectReader;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
struct State {
	artifact: tg::artifact::Id,
	progress: crate::progress::Handle<tg::artifact::checkout::Output>,
	ancestors: im::HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug)]
struct Arg {
	artifact: tg::artifact::Id,
	cache_path: PathBuf,
	depth: usize,
	temp_path: PathBuf,
}

impl Server {
	pub(crate) async fn cache_artifact(
		&self,
		artifact: tg::artifact::Id,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<()> {
		let ancestors = im::HashSet::default();
		self.cache_artifact_dependency(artifact, ancestors, progress)
			.await
			.and(Ok(()))
	}

	fn cache_artifact_dependency<'a>(
		&'a self,
		artifact: tg::artifact::Id,
		ancestors: im::HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
		progress: &'a crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> impl Future<Output = tg::Result<Progress>> + Send + 'a {
		async move {
			self.artifact_cache_task_map
				.get_or_spawn(artifact.clone(), {
					let server = self.clone();
					let progress = progress.clone();
					move |_| async move {
						server
							.cache_artifact_dependency_task(artifact, ancestors, &progress)
							.await
					}
				})
				.wait()
				.map(|result| match result {
					Ok(result) => Ok(result),
					Err(error) if error.is_cancelled() => {
						Ok(Err(tg::error!("the task was canceled")))
					},
					Err(error) => Err(error),
				})
				.await
				.unwrap()
		}
	}

	async fn cache_artifact_dependency_task(
		&self,
		artifact: tg::artifact::Id,
		ancestors: im::HashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<Progress> {
		// Create the path.
		let cache_path = self.cache_path().join(artifact.to_string());

		// If there is already a file system object at the cache path, then return.
		if tokio::fs::try_exists(&cache_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the path exists"))?
		{
			return Ok(Progress::new());
		}

		// Create the temp.
		let temp = Temp::new(self);
		let temp_path = temp.path().to_owned();

		// Create the state.
		let state = Arc::new(State {
			artifact: artifact.clone(),
			progress: progress.clone(),
			ancestors,
		});

		// Create the arg.
		let arg = Arg {
			artifact: artifact.clone(),
			cache_path: cache_path.clone(),
			depth: 0,
			temp_path: temp_path.clone(),
		};

		// Cache the artifact.
		let progress = self.cache_artifact_inner(&state, arg).await?;

		// Rename the temp to the path.
		let src = &temp_path;
		let dst = &cache_path;
		let result = tokio::fs::rename(src, dst).await;
		match result {
			Ok(()) => {},
			Err(error) if matches!(error.raw_os_error(), Some(libc::ENOTEMPTY | libc::EEXIST)) => {
			},
			Err(source) => {
				let src = src.display();
				let dst = dst.display();
				let error = tg::error!(!source, %src, %dst, "failed to rename to the cache path");
				return Err(error);
			},
		}

		// Set the file system object's modified time to the epoch.
		tokio::task::spawn_blocking({
			let path = cache_path.clone();
			move || {
				let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
				filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
				)?;
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		Ok(progress)
	}

	async fn cache_artifact_inner(&self, state: &State, arg: Arg) -> tg::Result<Progress> {
		let temp_path = arg.temp_path.clone();
		let artifact_id = arg.artifact.clone().into();

		// Check out the artifact.
		let mut progress = match arg.artifact.clone() {
			tg::artifact::Id::Directory(directory) => {
				self.cache_directory(state, arg, &directory).await?
			},
			tg::artifact::Id::File(file) => self.cache_file(state, arg, &file).await?,
			tg::artifact::Id::Symlink(symlink) => self.cache_symlink(state, arg, &symlink).await?,
		};

		// Set the file system object's modified time to the epoch.
		tokio::task::spawn_blocking({
			let path = temp_path.clone();
			move || {
				let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
				filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
				)?;
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		let metadata = self.get_object_metadata(&artifact_id).await;
		if let Ok(Metadata {
			complete: _,
			count: Some(count),
			depth: _,
			weight: Some(weight),
		}) = metadata
		{
			let bytes_increment = weight.saturating_sub(progress.bytes);
			let objects_increment = count.saturating_sub(progress.objects);
			state.progress.increment("objects", objects_increment);
			state.progress.increment("bytes", bytes_increment);
			progress.increment(objects_increment, bytes_increment);
		}
		Ok(progress)
	}
	async fn cache_directory(
		&self,
		state: &State,
		arg: Arg,
		directory: &tg::directory::Id,
	) -> tg::Result<Progress> {
		// Create the directory handle.
		let directory = tg::Directory::with_id(directory.clone());
		let mut progress = Progress::new();

		// Create the directory.
		tokio::fs::create_dir(&arg.temp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Recurse into the entries.
		let entries_progress = directory
			.entries(self)
			.await?
			.into_iter()
			.map(|(name, artifact)| {
				let server = self.clone();
				let state = state.clone();
				let arg = arg.clone();
				async move {
					let artifact = artifact.id(&server).await?;
					let cache_path = arg.cache_path.join(&name);
					let depth = arg.depth + 1;
					let temp_path = arg.temp_path.join(&name);
					let arg = Arg {
						artifact,
						cache_path,
						depth,
						temp_path,
					};
					let progress = server.cache_artifact_inner(&state, arg).await?;
					Ok::<_, tg::Error>(progress)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<Progress>>()
			.await?;

		progress = std::iter::once(&progress).chain(&entries_progress).fold(
			Progress::new(),
			|mut acc, progress| {
				acc.combine(progress);
				acc
			},
		);

		Ok(progress)
	}

	async fn cache_file(
		&self,
		state: &State,
		arg: Arg,
		file: &tg::file::Id,
	) -> tg::Result<Progress> {
		let file = tg::File::with_id(file.clone());
		let mut progress_out = Progress::new();

		// If the file is being cached at a depth in the cache directory greater than zero, then cache the file at depth zero and create a hard link.
		if arg.depth > 0 {
			let artifact = arg.artifact.clone();
			let ancestors = state.ancestors.clone();
			let progress = &state.progress;
			progress_out.combine(
				&self
					.cache_artifact_dependency(artifact.clone(), ancestors, progress)
					.await?,
			);
			let cache_path = self.cache_path().join(artifact.to_string());
			let hard_link_prohibited = arg
				.temp_path
				.to_str()
				.is_some_and(|path| path.contains(".app/Contents"));
			if hard_link_prohibited {
				tokio::fs::copy(&cache_path, &arg.temp_path)
					.await
					.map_err(|source| tg::error!(!source, "failed to copy the file"))?;
			} else {
				tokio::fs::hard_link(&cache_path, &arg.temp_path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the hard link"))?;
			}
			return Ok(progress_out);
		}

		// Check out the file's dependencies.
		let dep_progress = file
			.dependencies(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?
			.into_values()
			.filter_map(|referent| tg::Artifact::try_from(referent.item).ok())
			.map(|artifact| {
				let server = self.clone();
				let mut ancestors = state.ancestors.clone();
				async move {
					let artifact = artifact.id(&server).await?;
					let progress = if artifact != state.artifact && !ancestors.contains(&artifact) {
						ancestors.insert(state.artifact.clone());
						server
							.cache_artifact_dependency(artifact, ancestors, &state.progress)
							.await?
					} else {
						Progress::new()
					};
					Ok::<_, tg::Error>(progress)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<Progress>>()
			.await?;

		progress_out = std::iter::once(&progress_out).chain(&dep_progress).fold(
			Progress::new(),
			|mut acc, progress| {
				acc.combine(progress);
				acc
			},
		);

		// Create the file.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let mut reader = InspectReader::new(file.reader(self).await?, |slice| {
			state.progress.increment("bytes", slice.len() as u64);
			progress_out.increment(0, slice.len().to_u64().unwrap());
		});
		let mut file_ = tokio::fs::File::create(&arg.temp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the file"))?;
		tokio::io::copy(&mut reader, &mut file_)
			.await
			.map_err(|source| tg::error!(!source, "failed to write to the file"))?;
		drop(reader);
		drop(file_);
		drop(permit);

		// Make the file executable if necessary.
		if file.executable(self).await? {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(&arg.temp_path, permissions)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		// Set the extended attributes.
		let name = tg::file::XATTR_DATA_NAME;
		let data = file.data(self).await?;
		let json = serde_json::to_vec(&data)
			.map_err(|error| tg::error!(source = error, "failed to serialize the file data"))?;
		xattr::set(&arg.temp_path, name, &json).map_err(|source| {
			tg::error!(!source, "failed to set the extended attribute for the file")
		})?;

		Ok(progress_out)
	}

	async fn cache_symlink(
		&self,
		state: &State,
		arg: Arg,
		symlink: &tg::symlink::Id,
	) -> tg::Result<Progress> {
		let symlink = tg::Symlink::with_id(symlink.clone());
		let mut progress_out = Progress::new();

		// Get the symlink's target, artifact, and subpath.
		let target = symlink.target(self).await?;
		let artifact = symlink.artifact(self).await?;
		let subpath = symlink.subpath(self).await?;

		// If the symlink has an artifact, then check it out as a dependency.
		if let Some(artifact) = &artifact {
			let artifact = artifact.id(self).await?;
			if artifact != state.artifact && !state.ancestors.contains(&artifact) {
				let server = self.clone();
				let mut ancestors = state.ancestors.clone();
				ancestors.insert(state.artifact.clone());
				let progress = &state.progress;
				progress_out.combine(
					&Box::pin(server.cache_artifact_dependency(artifact, ancestors, progress))
						.await?,
				);
			}
		}

		// Render the target.
		let target = if let Some(target) = target {
			target
		} else if let Some(artifact) = artifact {
			// Render the target's absolute path.
			let artifact = artifact.id(self).await?;
			let mut target = self.cache_path().join(artifact.to_string());
			if let Some(subpath) = subpath {
				target = target.join(subpath);
			}

			// Diff the path with the symlink's parent.
			let src = arg.cache_path.parent().unwrap();
			let dst = &target;
			crate::util::path::diff(src, dst)?
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		tokio::fs::symlink(target, &arg.temp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		Ok(progress_out)
	}
}

impl Progress {
	pub fn new() -> Self {
		Self {
			objects: 0,
			bytes: 0,
		}
	}

	pub fn increment(&mut self, objects: u64, bytes: u64) {
		self.objects += objects;
		self.bytes += bytes;
	}

	pub fn combine(&mut self, other: &Progress) {
		self.increment(other.objects, other.bytes);
	}
}

impl Clone for Progress {
	fn clone(&self) -> Self {
		Self {
			objects: self.objects,
			bytes: self.objects,
		}
	}
}
