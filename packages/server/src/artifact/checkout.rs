use crate::{
	tmp::Tmp,
	util::{self, fs::remove},
	Server,
};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, Stream, TryStreamExt as _};
use std::{
	collections::BTreeSet, ffi::OsStr, os::unix::fs::PermissionsExt as _, path::PathBuf, sync::Arc,
};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::task::Task;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};

struct InnerArg<'a> {
	path: &'a PathBuf,
	artifact: &'a tg::Artifact,
	existing_artifact: Option<&'a tg::Artifact>,
	arg: &'a tg::artifact::checkout::Arg,
	depth: usize,
	files: Arc<DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>>,
	state: &'a tg::progress::State,
}

impl Server {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<PathBuf>>>> {
		// Get the metadata.
		let metadata = self.get_object_metadata(&id.clone().into()).await?;

		// Create the stream.
		let bars = [("objects", metadata.count), ("bytes", metadata.weight)];
		let stream = tg::progress::stream(
			{
				let server = self.clone();
				let id = id.clone();

				|state| async move {
					state.begin("objects").await;
					state.begin("bytes").await;
					server.check_out_artifact_task(&id, arg, state).await
				}
			},
			bars,
		);

		Ok(stream)
	}

	async fn check_out_artifact_task(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		state: tg::progress::State,
	) -> tg::Result<PathBuf> {
		// Get or spawn the task.
		let spawn = |_| {
			let server = self.clone();
			let id = id.clone();
			let arg = arg.clone();
			let files = Arc::new(DashMap::default());
			let state = state.clone();
			async move {
				server
					.check_out_artifact_with_files(&id, &arg, files, &state)
					.await
			}
		};
		let internal = arg.path.is_none();
		let task = if internal {
			self.checkout_task_map.get_or_spawn(id.clone(), spawn)
		} else {
			Task::spawn(spawn)
		};

		// Wait for the task.
		let path = task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the task failed"))??;

		Ok(path)
	}

	async fn check_out_artifact_with_files(
		&self,
		id: &tg::artifact::Id,
		arg: &tg::artifact::checkout::Arg,
		files: Arc<DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>>,
		state: &tg::progress::State,
	) -> tg::Result<std::path::PathBuf> {
		let artifact = tg::Artifact::with_id(id.clone());

		// Bundle the artifact if requested.
		let artifact = if arg.bundle {
			artifact
				.bundle(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to bundle the artifact"))?
		} else {
			artifact
		};

		if let Some(path) = arg.path.clone() {
			if !path.is_absolute() {
				return Err(tg::error!(%path = path.display(), "the path must be absolute"));
			}
			let exists = tokio::fs::try_exists(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to stat the path"),
			)?;
			if exists && !arg.force {
				return Err(
					tg::error!(%path = path.display(), "there is already a file system object at the path"),
				);
			}
			if (path.as_ref() as &std::path::Path).starts_with(self.checkouts_path()) {
				return Err(
					tg::error!(%path = path.display(), "cannot check out into the checkouts directory"),
				);
			}

			// Check in an existing artifact at the path.
			let existing_artifact = if exists {
				let arg = tg::artifact::checkin::Arg {
					destructive: false,
					deterministic: true,
					locked: true,
					path: path.clone(),
				};
				let artifact = tg::Artifact::check_in(self, arg).await?;
				Some(artifact)
			} else {
				None
			};

			// Perform the checkout.
			let arg = InnerArg {
				path: &path,
				artifact: &artifact,
				existing_artifact: existing_artifact.as_ref(),
				depth: 0,
				arg,
				files,
				state,
			};
			self.check_out_inner(arg).await?;

			Ok(path)
		} else {
			// Get the path in the checkouts directory.
			let id = artifact.id(self).await?;
			let path: std::path::PathBuf = self.checkouts_path().join(id.to_string());
			let artifact_path = self.artifacts_path().join(id.to_string());

			// If there is already a file system object at the path, then return.
			if tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to stat the path"))?
			{
				return Ok(artifact_path);
			}

			// If the VFS is enabled and `force` is `false`, then return.
			if self.options.vfs.is_some() && !arg.force {
				return Ok(artifact_path);
			}

			// Create a tmp.
			let tmp = Tmp::new(self);

			// Perform the checkout to the tmp.
			let files = Arc::new(DashMap::default());
			let arg = InnerArg {
				path: &tmp.path,
				artifact: &artifact,
				existing_artifact: None,
				depth: 0,
				arg,
				files,
				state,
			};
			self.check_out_inner(arg).await?;

			// Move the checkout to the checkouts directory.
			match tokio::fs::rename(&tmp, &path).await {
				Ok(()) => (),
				// If the entry in the checkouts directory exists, then remove the checkout to the tmp.
				Err(ref error)
					if matches!(error.raw_os_error(), Some(libc::ENOTEMPTY | libc::EEXIST)) =>
				{
					remove(&tmp).await.ok();
				},

				// Otherwise, return the error.
				Err(source) => {
					return Err(
						tg::error!(!source, %tmp = tmp.path.display(), %path = path.display(), "failed to move the checkout to the checkouts directory"),
					);
				},
			};

			Ok(artifact_path)
		}
	}

	async fn check_out_inner(&self, arg: InnerArg<'_>) -> tg::Result<()> {
		let InnerArg {
			path,
			artifact,
			existing_artifact,
			arg,
			depth,
			files,
			state,
		} = arg;

		// If the artifact is the same as the existing artifact, then return.
		let id = artifact.id(self).await?;
		match existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if id == existing_artifact.id(self).await? {
					let metadata = self.get_object_metadata(&id.clone().into()).await?;
					if let Some(count) = metadata.count {
						state.report_progress("objects", count).ok();
					}
					if let Some(weight) = metadata.weight {
						state.report_progress("bytes", weight).ok();
					}
					return Ok(());
				}
			},
		}

		// Call the appropriate function for the artifact's type.
		let arg_ = InnerArg {
			path,
			artifact,
			existing_artifact,
			arg,
			depth,
			files,
			state,
		};
		match artifact {
			tg::Artifact::Directory(_) => {
				Box::pin(self.check_out_directory(arg_)).await.map_err(
					|source| tg::error!(!source, %id, %path = path.display(), "failed to check out the directory"),
				)?;
			},

			tg::Artifact::File(_) => {
				Box::pin(self.check_out_file(arg_)).await.map_err(
					|source| tg::error!(!source, %id, %path = path.display(), "failed to check out the file"),
				)?;
			},

			tg::Artifact::Symlink(_) => {
				Box::pin(self.check_out_symlink(arg_)).await.map_err(
					|source| tg::error!(!source, %id, %path = path.display(), "failed to check out the symlink"),
				)?;
			},
		}

		// If this is an internal checkout, then set the file system object's modified time to the epoch.
		if arg.path.is_none() {
			tokio::task::spawn_blocking({
				let path = path.clone();
				move || {
					let epoch =
						filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
					filetime::set_symlink_file_times(path, epoch, epoch)
						.map_err(|source| tg::error!(!source, "failed to set the modified time"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.await
			.unwrap()?;
		}

		// Send a new progress report.
		state.report_progress("objects", 1).ok();

		Ok(())
	}

	async fn check_out_directory(&self, arg: InnerArg<'_>) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			depth,
			existing_artifact,
			files,
			path,
			state,
		} = arg;
		let directory = artifact
			.try_unwrap_directory_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a directory"))?;

		// Handle an existing artifact at the path.
		match existing_artifact {
			// If there is an existing directory, then remove any extraneous entries.
			Some(tg::Artifact::Directory(existing_directory)) => {
				existing_directory
					.entries(self)
					.await?
					.keys()
					.map(|name| async move {
						if !directory.entries(self).await?.contains_key(name) {
							let entry_path = path.clone().join(name);
							remove(&entry_path).await.ok();
						}
						Ok::<_, tg::Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await?;
			},

			// If there is an existing file system object at the path and it is not a directory, then remove it, create a directory, and continue.
			Some(_) => {
				remove(path).await.ok();
				tokio::fs::create_dir_all(path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},

			// If there is no artifact at this path, then create a directory.
			None => {
				tokio::fs::create_dir_all(path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},
		}

		// Recurse into the entries.
		directory
			.entries(self)
			.await?
			.iter()
			.map(|(name, artifact)| {
				let existing_artifact = &existing_artifact;
				let files = files.clone();
				async move {
					// Retrieve an existing artifact.
					let existing_artifact = match existing_artifact {
						Some(tg::Artifact::Directory(existing_directory)) => {
							let name = name
								.parse()
								.map_err(|source| tg::error!(!source, "invalid entry name"))?;
							existing_directory.try_get(self, &name).await?
						},
						_ => None,
					};

					// Recurse.
					let entry_path = path.clone().join(name);
					let arg = InnerArg {
						path: &entry_path,
						artifact,
						existing_artifact: existing_artifact.as_ref(),
						depth: depth + 1,
						arg,
						files,
						state,
					};
					self.check_out_inner(arg).await?;

					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		Ok(())
	}

	async fn check_out_file(&self, arg: InnerArg<'_>) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			existing_artifact,
			files,
			path,
			state,
			..
		} = arg;
		let file = artifact
			.try_unwrap_file_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a file"))?;

		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				remove(path).await.ok();
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Check out the file's dependencies.
		let dependencies = artifact
			.dependencies(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?
			.iter()
			.map(|artifact| artifact.id(self))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<BTreeSet<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?;
		if arg.dependencies && !dependencies.is_empty() {
			dependencies
				.iter()
				.map(|artifact| async {
					let arg = tg::artifact::checkout::Arg {
						bundle: false,
						force: false,
						path: None,
						dependencies: true,
					};
					Box::pin(self.check_out_artifact_with_files(
						artifact,
						&arg,
						files.clone(),
						state,
					))
					.await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to check out the file's dependencies")
				})?;
		}

		// Attempt to copy the file from another file in the checkout.
		let id = file.id(self).await?;
		let existing_path = files.get(&id).map(|path| path.clone());
		if let Some(existing_path) = existing_path {
			let permit = self.file_descriptor_semaphore.acquire().await;
			tokio::fs::copy(&existing_path, &path).await.map_err(
				|source| tg::error!(!source, %existing_path = path.display(), %to = path.display(), %id, "failed to copy the file"),
			)?;
			drop(permit);
			return Ok(());
		}

		// Attempt to use the file from an internal checkout.
		let internal_checkout_path = self.checkouts_path().join(id.to_string());
		if arg.path.is_none() {
			// If this checkout is internal, then create a hard link.
			let result = tokio::fs::hard_link(&internal_checkout_path, path).await;
			if result.is_ok() {
				return Ok(());
			}
		} else {
			// If this checkout is external, then copy the file.
			let permit = self.file_descriptor_semaphore.acquire().await;
			let result = tokio::fs::copy(&internal_checkout_path, path).await;
			drop(permit);
			if result.is_ok() {
				return Ok(());
			}
		}

		// Create the file.
		let permit = self.file_descriptor_semaphore.acquire().await;
		tokio::io::copy(
			&mut file.reader(self).await?,
			&mut tokio::fs::File::create(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the file"))?,
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to write the bytes"))?;
		drop(permit);

		// Make the file executable if necessary.
		if file.executable(self).await? {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(path, permissions)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		// Set the extended attributes.
		let name = tg::file::XATTR_NAME;
		let xattr = file.data(self).await?;
		let json = serde_json::to_vec(&xattr)
			.map_err(|error| tg::error!(source = error, "failed to serialize the dependencies"))?;
		xattr::set(path, name, &json).map_err(|source| {
			tg::error!(!source, "failed to set the extended attribute for the file")
		})?;

		// Add the path to the files map.
		files.insert(id.clone(), path.clone());

		Ok(())
	}

	async fn check_out_symlink(&self, arg: InnerArg<'_>) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			depth,
			existing_artifact,
			files,
			path,
			state,
		} = arg;
		let symlink = artifact
			.try_unwrap_symlink_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a symlink"))?;

		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				remove(&path).await.ok();
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Check out the symlink's artifact if necessary.
		if arg.dependencies {
			if let Some(artifact) = symlink.artifact(self).await? {
				if arg.path.is_some() {
					return Err(tg::error!(
						r#"cannot perform an external check out of a symlink with an artifact"#
					));
				}
				let id = artifact.id(self).await?;
				let arg = tg::artifact::checkout::Arg::default();
				Box::pin(self.check_out_artifact_with_files(&id, &arg, files, state)).await?;
			}
		}

		// Render the target.
		let mut target = std::path::PathBuf::new();
		let artifact = symlink.artifact(self).await?;
		let path_ = symlink.path(self).await?;
		if let Some(artifact) = artifact.as_ref() {
			for _ in 0..depth {
				target.push(std::path::Component::ParentDir);
			}
			target = target.join("../../.tangram/artifacts");
			let id = &artifact.id(self).await?.to_string();
			let component = std::path::Component::Normal(OsStr::new(id));
			target.push(component);
		}
		if let Some(path) = path_.as_ref() {
			target = target.join(path.clone());
		}

		// Create the symlink.
		tokio::fs::symlink(target, path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_check_out_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let stream = handle.check_out_artifact(&id, arg).await?;
		Ok(util::progress::sse(stream))
	}
}
