use crate::Server;
use dashmap::{DashMap, DashSet};
use futures::{stream::FuturesUnordered, Future, TryStreamExt as _};
use std::{os::unix::fs::PermissionsExt as _, path::PathBuf, sync::Arc};
use tangram_client as tg;

#[derive(Clone, Debug)]
struct State {
	artifacts_path: Option<PathBuf>,
	files: DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>,
	#[allow(unused)]
	progress: crate::progress::Handle<tg::artifact::checkout::Output>,
	visited_dependencies: DashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug)]
struct Arg {
	artifact: tg::artifact::Id,
	existing_artifact: Option<tg::Artifact>,
	path: PathBuf,
	root_artifact: tg::artifact::Id,
	root_path: Arc<PathBuf>,
}

impl Server {
	pub(crate) async fn check_out_artifact_external(
		&self,
		artifact: tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		path: PathBuf,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
		// Canonicalize the path's parent.
		let path = crate::util::fs::canonicalize_parent(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path's parent"))?;

		// Determine the artifacts path.
		let artifacts_path = if artifact.is_directory() {
			Some(path.join(".tangram/artifacts"))
		} else {
			None
		};

		// Check if an artifact exists at the path.
		let exists = tokio::fs::try_exists(&path).await.unwrap_or(false);

		// If an artifact exists, and this is not a forced checkout, then return an error.
		if exists && !arg.force {
			return Err(tg::error!("there is an existing artifact"));
		}

		// Attempt to check in the existing artifact if there is one.
		let existing_artifact = if exists {
			// Attempt to check in the existing artifact.
			let arg = tg::artifact::checkin::Arg {
				destructive: false,
				deterministic: true,
				ignore: true,
				locked: true,
				path: path.clone(),
			};
			let option = tg::Artifact::check_in(self, arg).await.ok();

			// If the checkin failed, then remove the file system object at the path.
			if option.is_none() {
				crate::util::fs::remove(&path).await.map_err(|source| {
					tg::error!(!source, "failed to remove the existing artifact")
				})?;
			}

			option
		} else {
			None
		};

		// Create the state.
		let state = Arc::new(State {
			artifacts_path,
			files: DashMap::default(),
			progress: progress.clone(),
			visited_dependencies: DashSet::default(),
		});

		// Create the arg.
		let arg = Arg {
			artifact: artifact.clone(),
			existing_artifact,
			path: path.clone(),
			root_artifact: artifact.clone(),
			root_path: Arc::new(path.clone()),
		};

		// Perform the checkout.
		self.check_out_artifact_external_inner(&state, arg).await?;

		// Create a lockfile and write it if it is not empty.
		let artifact = tg::Artifact::with_id(artifact.clone());
		let lockfile = self
			.create_lockfile_for_artifact(&artifact)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the lockfile"))?;
		if !lockfile.nodes.is_empty() {
			let lockfile_path = if matches!(artifact, tg::Artifact::Directory(_)) {
				path.join(tg::package::LOCKFILE_FILE_NAME)
			} else {
				path.parent().unwrap().join(tg::package::LOCKFILE_FILE_NAME)
			};
			let contents = serde_json::to_vec(&lockfile)
				.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;
			let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			tokio::fs::write(&lockfile_path, &contents).await.map_err(
				|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write the lockfile"),
			)?;
			drop(permit);
		}

		// Create the output.
		let output = tg::artifact::checkout::Output { path };

		Ok(output)
	}

	async fn check_out_artifact_external_inner(&self, state: &State, arg: Arg) -> tg::Result<()> {
		// If the artifact is the same as the existing artifact, then return.
		match &arg.existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if arg.artifact == existing_artifact.id(self).await? {
					return Ok(());
				}
			},
		}

		// Check out the artifact.
		match arg.artifact.clone() {
			tg::artifact::Id::Directory(directory) => {
				self.check_out_external_directory(state, arg, &directory)
					.await?;
			},
			tg::artifact::Id::File(file) => {
				self.check_out_external_file(state, arg, &file).await?;
			},
			tg::artifact::Id::Symlink(symlink) => {
				self.check_out_external_symlink(state, arg, &symlink)
					.await?;
			},
		};

		Ok(())
	}

	async fn check_out_artifact_external_dependency(
		&self,
		state: &State,
		artifact: tg::artifact::Id,
	) -> tg::Result<()> {
		// Mark the dependency as visited and exit early if it has already been visited.
		if !state.visited_dependencies.insert(artifact.clone()) {
			return Ok(());
		}

		// Get the artifacts path.
		let artifacts_path = state.artifacts_path.as_ref().ok_or_else(|| {
			tg::error!("cannot check out a dependency if the artifact is not a directory")
		})?;

		// Create the artifacts directory.
		tokio::fs::create_dir_all(&artifacts_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the artifacts directory"))?;

		// Create the arg.
		let path = artifacts_path.join(artifact.to_string());
		let arg = Arg {
			artifact: artifact.clone(),
			existing_artifact: None,
			path: path.clone(),
			root_artifact: artifact.clone(),
			root_path: Arc::new(path.clone()),
		};

		// Perform the checkout.
		self.check_out_artifact_external_inner(state, arg).await?;

		Ok(())
	}

	async fn check_out_external_directory(
		&self,
		state: &State,
		arg: Arg,
		directory: &tg::directory::Id,
	) -> tg::Result<()> {
		// Create the directory handle.
		let directory = tg::Directory::with_id(directory.clone());

		// Handle an existing artifact at the path.
		match &arg.existing_artifact {
			// If there is an existing directory, then remove any extraneous entries.
			Some(tg::Artifact::Directory(existing_directory)) => {
				existing_directory
					.entries(self)
					.await?
					.keys()
					.map(|name| {
						let directory = directory.clone();
						let path = arg.path.clone();
						async move {
							if !directory.entries(self).await?.contains_key(name) {
								let path = path.join(name);
								crate::util::fs::remove(&path).await.ok();
							}
							Ok::<_, tg::Error>(())
						}
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await?;
			},

			// If there is an existing file system object at the path and it is not a directory, then remove it, create a directory, and continue.
			Some(_) => {
				crate::util::fs::remove(&arg.path).await.ok();
				tokio::fs::create_dir_all(&arg.path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},

			// If there is no artifact at this path, then create a directory.
			None => {
				tokio::fs::create_dir_all(&arg.path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},
		}

		// Recurse into the entries.
		directory
			.entries(self)
			.await?
			.into_iter()
			.map(|(name, artifact)| {
				let server = self.clone();
				let state = state.clone();
				let arg = arg.clone();
				let existing_artifact = arg.existing_artifact.clone();
				async move {
					let artifact = artifact.id(&server).await?;
					let existing_artifact =
						if let Some(tg::Artifact::Directory(existing_directory)) =
							&existing_artifact
						{
							existing_directory.try_get_entry(&server, &name).await?
						} else {
							None
						};
					let path = arg.path.join(&name);
					let arg = Arg {
						artifact,
						existing_artifact,
						path,
						root_artifact: arg.root_artifact,
						root_path: arg.root_path,
					};
					#[allow(clippy::manual_async_fn)]
					fn future(
						server: Server,
						state: State,
						arg: Arg,
					) -> impl Future<Output = tg::Result<()>> + Send + 'static {
						async move { server.check_out_artifact_external_inner(&state, arg).await }
					}
					tokio::spawn(future(self.clone(), state.clone(), arg))
						.await
						.unwrap()?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		Ok(())
	}

	async fn check_out_external_file(
		&self,
		state: &State,
		arg: Arg,
		file: &tg::file::Id,
	) -> tg::Result<()> {
		let file = tg::File::with_id(file.clone());

		// Handle an existing artifact at the path.
		if arg.existing_artifact.is_some() {
			crate::util::fs::remove(&arg.path).await.ok();
		};

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

					#[allow(clippy::manual_async_fn)]
					fn future(
						server: Server,
						state: State,
						artifact: tg::artifact::Id,
					) -> impl Future<Output = tg::Result<()>> + Send + 'static {
						async move {
							server
								.check_out_artifact_external_dependency(&state, artifact)
								.await
						}
					}
					tokio::spawn(future(server, state, artifact))
						.await
						.unwrap()?;

					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		// Attempt to copy the file from another file in the checkout.
		let path = state
			.files
			.get(&file.id(self).await?)
			.map(|path| path.clone());
		if let Some(path) = path {
			let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			tokio::fs::copy(&path, &arg.path).await.map_err(
				|source| tg::error!(!source, %src = path.display(), %dst = &arg.path.display(), %file, "failed to copy the file"),
			)?;
			drop(permit);
			return Ok(());
		}

		// Attempt to copy the file from the cache directory.
		let cache_path = self.cache_path().join(file.id(self).await?.to_string());
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let result = tokio::fs::copy(&cache_path, &arg.path).await;
		drop(permit);
		if result.is_ok() {
			return Ok(());
		}

		// Otherwise, create the file.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let mut reader = file.reader(self).await?;
		let mut file_ = tokio::fs::File::create(&arg.path)
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
			tokio::fs::set_permissions(&arg.path, permissions)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		// Set the extended attributes.
		let name = tg::file::XATTR_DATA_NAME;
		let data = file.data(self).await?;
		let json = serde_json::to_vec(&data)
			.map_err(|error| tg::error!(source = error, "failed to serialize the file data"))?;
		xattr::set(&arg.path, name, &json).map_err(|source| {
			tg::error!(!source, "failed to set the extended attribute for the file")
		})?;

		// If the artifact is a file, then add its path to the files map.
		if let Ok(file) = arg.artifact.try_unwrap_file_ref() {
			if let dashmap::Entry::Vacant(entry) = state.files.entry(file.clone()) {
				entry.insert(arg.path.clone());
			}
		}

		Ok(())
	}

	async fn check_out_external_symlink(
		&self,
		state: &State,
		arg: Arg,
		symlink: &tg::symlink::Id,
	) -> tg::Result<()> {
		let symlink = tg::Symlink::with_id(symlink.clone());

		// Handle an existing artifact at the path.
		if arg.existing_artifact.is_some() {
			crate::util::fs::remove(&arg.path).await.ok();
		};

		// Get the symlink's target, artifact, and subpath.
		let target = symlink.target(self).await?;
		let artifact = symlink.artifact(self).await?;
		let subpath = symlink.subpath(self).await?;

		// If the symlink has an artifact and it is not the current root, then check it out as a dependency.
		if let Some(artifact) = &artifact {
			if artifact.id(self).await? != arg.root_artifact {
				let server = self.clone();
				let artifact = artifact.id(self).await?;
				#[allow(clippy::manual_async_fn)]
				fn future(
					server: Server,
					state: State,
					artifact: tg::artifact::Id,
				) -> impl Future<Output = tg::Result<()>> + Send + 'static {
					async move {
						server
							.check_out_artifact_external_dependency(&state, artifact)
							.await
					}
				}
				tokio::spawn(future(server, state.clone(), artifact))
					.await
					.unwrap()?;
			}
		}

		// Render the target.
		let target = if let Some(target) = target {
			target
		} else if let Some(artifact) = artifact {
			// Render the target's absolute path.
			let mut target = if artifact.id(self).await? == arg.root_artifact {
				// If the symlink's artifact is the same as the current root, then use the current root's path.
				arg.root_path.as_ref().to_owned()
			} else {
				// Otherwise, use the artifact's path in the artifacts directory.
				let artifacts_path = state.artifacts_path.as_ref().ok_or_else(|| {
					tg::error!("cannot check out an artifact symlink without an artifacts path")
				})?;
				let id = artifact.id(self).await?;
				artifacts_path.join(id.to_string())
			};
			if let Some(subpath) = subpath {
				target = target.join(subpath);
			}

			// Diff the path with the symlink's parent.
			let src = arg.path.parent().unwrap();
			let dst = &target;
			crate::util::path::diff(src, dst)?
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		tokio::fs::symlink(target, &arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		Ok(())
	}
}
