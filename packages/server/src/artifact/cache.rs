use crate::{temp::Temp, Server};
use futures::{stream::FuturesUnordered, Future, FutureExt as _, TryStreamExt as _};
use num::ToPrimitive as _;
use std::{os::unix::fs::PermissionsExt as _, path::PathBuf, sync::Arc};
use tangram_client::{self as tg, handle::Ext as _};
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

#[derive(Clone, Debug)]
pub(crate) struct Output {
	progress: Progress,
}

#[derive(Clone, Debug, Default)]
struct Progress {
	objects: u64,
	bytes: u64,
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
	) -> impl Future<Output = tg::Result<Output>> + Send + 'a {
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
	) -> tg::Result<Output> {
		// Create the path.
		let cache_path = self.cache_path().join(artifact.to_string());

		// If there is already a file system object at the cache path, then return.
		if tokio::fs::try_exists(&cache_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the path exists"))?
		{
			let output = Output {
				progress: Progress::default(),
			};
			return Ok(output);
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
		let output = self.cache_artifact_inner(&state, arg).await?;

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

		Ok(output)
	}

	async fn cache_artifact_inner(&self, state: &State, arg: Arg) -> tg::Result<Output> {
		let temp_path = arg.temp_path.clone();
		let artifact_id = arg.artifact.clone().into();

		// Check out the artifact.
		let mut output = match arg.artifact.clone() {
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

		// Increment the progress.
		let metadata = self.get_object_metadata(&artifact_id).await;
		if let Ok(metadata) = metadata {
			if let Some(count) = metadata.count {
				let objects = count.saturating_sub(output.progress.objects);
				output.progress.objects += objects;
				state.progress.increment("objects", objects);
			}
			if let Some(weight) = metadata.weight {
				let bytes = weight.saturating_sub(output.progress.bytes);
				output.progress.bytes += bytes;
				state.progress.increment("bytes", bytes);
			}
		}

		Ok(output)
	}
	async fn cache_directory(
		&self,
		state: &State,
		arg: Arg,
		directory: &tg::directory::Id,
	) -> tg::Result<Output> {
		let mut output = Output {
			progress: Progress::default(),
		};
		let directory = tg::Directory::with_id(directory.clone());

		// Create the directory.
		tokio::fs::create_dir(&arg.temp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Recurse into the entries.
		let outputs = directory
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
			.try_collect::<Vec<Output>>()
			.await?;
		output.progress += outputs.into_iter().map(|output| output.progress).sum();

		Ok(output)
	}

	async fn cache_file(&self, state: &State, arg: Arg, file: &tg::file::Id) -> tg::Result<Output> {
		let mut output = Output {
			progress: Progress::default(),
		};
		let file = tg::File::with_id(file.clone());

		// If the file is being cached at a depth in the cache directory greater than zero, then cache the file at depth zero and create a hard link.
		if arg.depth > 0 {
			let artifact = arg.artifact.clone();
			let ancestors = state.ancestors.clone();
			let progress = &state.progress;
			let dependency_output = self
				.cache_artifact_dependency(artifact.clone(), ancestors, progress)
				.await?;
			output.progress += dependency_output.progress;
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
			return Ok(output);
		}

		// Check out the file's dependencies.
		let dependency_outputs = file
			.dependencies(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?
			.into_values()
			.filter_map(|referent| tg::Artifact::try_from(referent.item).ok())
			.map(|artifact| {
				let server = self.clone();
				let mut ancestors = state.ancestors.clone();
				async move {
					let mut output = Output {
						progress: Progress::default(),
					};
					let artifact = artifact.id(&server).await?;
					if artifact != state.artifact && !ancestors.contains(&artifact) {
						ancestors.insert(state.artifact.clone());
						let dependency_output = server
							.cache_artifact_dependency(artifact, ancestors, &state.progress)
							.await?;
						output.progress += dependency_output.progress;
					}
					Ok::<_, tg::Error>(output)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<Output>>()
			.await?;
		output.progress += dependency_outputs
			.into_iter()
			.map(|output| output.progress)
			.sum();

		// Create the file.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let reader = file.read(self, tg::blob::read::Arg::default()).await?;
		let mut reader = InspectReader::new(reader, |slice| {
			output.progress.bytes += slice.len().to_u64().unwrap();
			state.progress.increment("bytes", slice.len() as u64);
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

		Ok(output)
	}

	async fn cache_symlink(
		&self,
		state: &State,
		arg: Arg,
		symlink: &tg::symlink::Id,
	) -> tg::Result<Output> {
		let mut output = Output {
			progress: Progress::default(),
		};
		let symlink = tg::Symlink::with_id(symlink.clone());

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
				let dependency_output =
					Box::pin(server.cache_artifact_dependency(artifact, ancestors, progress))
						.await?;
				output.progress += dependency_output.progress;
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

		Ok(output)
	}
}

impl std::ops::Add for Progress {
	type Output = Self;

	fn add(self, rhs: Self) -> Self::Output {
		Self::Output {
			objects: self.objects + rhs.objects,
			bytes: self.bytes + rhs.bytes,
		}
	}
}

impl std::ops::AddAssign for Progress {
	fn add_assign(&mut self, rhs: Self) {
		self.objects += rhs.objects;
		self.bytes += rhs.bytes;
	}
}

impl std::iter::Sum for Progress {
	fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
		iter.fold(Self::default(), |a, b| a + b)
	}
}
