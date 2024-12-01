use crate::{temp::Temp, Server};
use futures::{stream::FuturesUnordered, Future, TryStreamExt as _};
use std::{os::unix::fs::PermissionsExt as _, path::PathBuf, sync::Arc};
use tangram_client as tg;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
struct State {
	artifact: tg::artifact::Id,
	cache_path: PathBuf,
	#[allow(unused)]
	progress: crate::progress::Handle<tg::artifact::checkout::Output>,
	#[allow(unused)]
	temp_path: PathBuf,
}

#[derive(Clone, Debug)]
struct Arg {
	artifact: tg::artifact::Id,
	cache_path: PathBuf,
	temp_path: PathBuf,
}

impl Server {
	pub(crate) async fn cache_artifact(
		&self,
		artifact: tg::artifact::Id,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<()> {
		self.cache_artifact_dependency(artifact, progress).await
	}

	fn cache_artifact_dependency<'a>(
		&'a self,
		artifact: tg::artifact::Id,
		progress: &'a crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> impl Future<Output = tg::Result<()>> + Send + 'a {
		async move {
			self.artifact_cache_task_map
				.get_or_spawn(artifact.clone(), {
					let server = self.clone();
					let progress = progress.clone();
					move |_| async move {
						server
							.cache_artifact_dependency_task(artifact, &progress)
							.await
					}
				})
				.wait()
				.await
				.unwrap()
		}
	}

	async fn cache_artifact_dependency_task(
		&self,
		artifact: tg::artifact::Id,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<()> {
		// Create the path.
		let cache_path = self.cache_path().join(artifact.to_string());

		// Create the temp.
		let temp = Temp::new(self);
		let temp_path = temp.path().to_owned();

		// Create the state.
		let state = Arc::new(State {
			artifact: artifact.clone(),
			cache_path: cache_path.clone(),
			progress: progress.clone(),
			temp_path: temp_path.clone(),
		});

		// Create the arg.
		let arg = Arg {
			artifact: artifact.clone(),
			cache_path: cache_path.clone(),
			temp_path: temp_path.clone(),
		};

		// Cache the artifact.
		self.cache_artifact_inner(&state, arg).await?;

		// Rename the temp to the path.
		let src = &temp_path;
		let dst = &cache_path;
		let result = tokio::fs::rename(src, dst).await;
		match result {
			Ok(()) => (),
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

		Ok(())
	}

	async fn cache_artifact_inner(&self, state: &State, arg: Arg) -> tg::Result<()> {
		let temp_path = arg.temp_path.clone();

		// Check out the artifact.
		match arg.artifact.clone() {
			tg::artifact::Id::Directory(directory) => {
				self.cache_directory(state, arg, &directory).await?;
			},
			tg::artifact::Id::File(file) => {
				self.cache_file(state, arg, &file).await?;
			},
			tg::artifact::Id::Symlink(symlink) => {
				self.cache_symlink(state, arg, &symlink).await?;
			},
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

		Ok(())
	}
	async fn cache_directory(
		&self,
		state: &State,
		arg: Arg,
		directory: &tg::directory::Id,
	) -> tg::Result<()> {
		// Create the directory handle.
		let directory = tg::Directory::with_id(directory.clone());

		// Create the directory.
		tokio::fs::create_dir(&arg.temp_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Recurse into the entries.
		directory
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
					let temp_path = arg.temp_path.join(&name);
					let arg = Arg {
						artifact,
						cache_path,
						temp_path,
					};
					server.cache_artifact_inner(&state, arg).await?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		Ok(())
	}

	async fn cache_file(&self, state: &State, arg: Arg, file: &tg::file::Id) -> tg::Result<()> {
		let file = tg::File::with_id(file.clone());

		// Check out the file's dependencies.
		file.dependencies(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?
			.into_values()
			.filter_map(|referent| tg::Artifact::try_from(referent.item).ok())
			.map(|artifact| {
				let server = self.clone();
				let state = state.clone();
				async move {
					let artifact = artifact.id(&server).await?;
					server
						.cache_artifact_dependency(artifact, &state.progress)
						.await?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		// Attempt to copy the file from the cache directory.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let result = tokio::fs::copy(&arg.cache_path, &arg.temp_path).await;
		drop(permit);
		if result.is_ok() {
			return Ok(());
		}

		// Otherwise, create the file.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let mut reader = file.reader(self).await?;
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

		Ok(())
	}

	async fn cache_symlink(
		&self,
		state: &State,
		arg: Arg,
		symlink: &tg::symlink::Id,
	) -> tg::Result<()> {
		let symlink = tg::Symlink::with_id(symlink.clone());

		// Get the symlink's target, artifact, and subpath.
		let target = symlink.target(self).await?;
		let artifact = symlink.artifact(self).await?;
		let subpath = symlink.subpath(self).await?;

		// If the symlink has an artifact and it is not the current root, then check it out as a dependency.
		if let Some(artifact) = &artifact {
			if artifact.id(self).await? != state.artifact {
				let server = self.clone();
				let artifact = artifact.id(self).await?;
				Box::pin(server.cache_artifact_dependency(artifact, &state.progress)).await?;
			}
		}

		// Render the target.
		let target = if let Some(target) = target {
			target
		} else if let Some(artifact) = artifact {
			// Render the target's absolute path.
			let mut target = if artifact.id(self).await? == state.artifact {
				// If the symlink's artifact is the same as the current root, then use the current root's path.
				state.cache_path.clone()
			} else {
				// Otherwise, use the artifact's path in the cache directory.
				let id = artifact.id(self).await?;
				self.cache_path().join(id.to_string())
			};
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

		Ok(())
	}
}
