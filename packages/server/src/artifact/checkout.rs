use crate::{temp::Temp, Server};
use dashmap::{DashMap, DashSet};
use futures::{stream::FuturesUnordered, FutureExt, Stream, StreamExt as _, TryStreamExt as _};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, BTreeSet},
	future::Future,
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	pin::pin,
	sync::Arc,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::{stream::TryStreamExt as _, task::Task};
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};

#[cfg(test)]
mod tests;

#[derive(Clone)]
struct InnerArg {
	arg: tg::artifact::checkout::Arg,
	artifact: tg::Artifact,
	root_artifact: tg::Artifact,
	cache_path: PathBuf,
	existing_artifact: Option<tg::Artifact>,
	files: Arc<DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>>,
	temp_path: PathBuf,
	final_path: PathBuf,
	root_path: PathBuf,
	progress: crate::progress::Handle<tg::artifact::checkout::Output>,
	visited: Arc<DashSet<PathBuf, fnv::FnvBuildHasher>>,
}

impl Server {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>,
	> {
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let progress = progress.clone();
			async move { server.check_out_artifact_task(&id, arg, &progress).await }
		});
		tokio::spawn({
			let progress = progress.clone();
			async move {
				match task.await {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(source) => {
						progress.error(tg::error!(!source, "the task panicked"));
					},
				};
			}
		});
		let stream = progress.stream();
		Ok(stream)
	}

	async fn check_out_artifact_task(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
		// Get or spawn the task.
		let spawn = |_| {
			let server = self.clone();
			let id = id.clone();
			let arg = arg.clone();
			let files = Arc::new(DashMap::default());
			let visited = Arc::new(DashSet::default());
			let progress = progress.clone();

			async move {
				// Compute the cache path.
				let cache_path = server
					.get_cache_path_for_path(&id, arg.path.as_deref())
					.await?;

				// Checkout the artifact.
				let output = server
					.check_out_artifact_with_files(
						&id,
						&arg,
						&cache_path,
						files,
						visited,
						&progress,
					)
					.await?;

				if let Some(path) = arg.path {
					// Create the lockfile for external checkouts.
					let artifact = tg::Artifact::with_id(id.clone());
					let lockfile = server
						.create_lockfile_with_artifact(&artifact)
						.await
						.map_err(|source| tg::error!(!source, "failed to create the lockfile"))?;

					// Write the lockfile if it is not empty.
					if !lockfile.nodes.is_empty() {
						let lockfile_path = if matches!(artifact, tg::Artifact::Directory(_)) {
							path.join("tangram.lock")
						} else {
							path.parent().unwrap().join("tangram.lock")
						};

						let contents = serde_json::to_vec(&lockfile).map_err(|source| {
							tg::error!(!source, "failed to serialize lockfile")
						})?;
						let _permit = server.file_descriptor_semaphore.acquire().await.unwrap();
						tokio::fs::write(&lockfile_path, &contents).await.map_err(
							|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write lockfile"),
						)?;
					}
				}

				Ok(output)
			}
		};
		let internal = arg.path.is_none();
		let task = if internal {
			self.checkout_task_map.get_or_spawn(id.clone(), spawn)
		} else {
			Task::spawn(spawn)
		};

		// Wait for the task.
		let output = task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the task failed"))??;

		Ok(output)
	}

	async fn check_out_artifact_with_files(
		&self,
		id: &tg::artifact::Id,
		arg: &tg::artifact::checkout::Arg,
		cache_path: &Path,
		files: Arc<DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>>,
		visited: Arc<DashSet<PathBuf, fnv::FnvBuildHasher>>,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
		// Get the artifact.
		let artifact = tg::Artifact::with_id(id.clone());

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
			if (path.as_ref() as &Path).starts_with(self.cache_path()) {
				return Err(
					tg::error!(%path = path.display(), "cannot check out an artifact to the cache path"),
				);
			}

			// Check in an existing artifact at the path.
			let existing_artifact = if exists {
				let arg = tg::artifact::checkin::Arg {
					destructive: false,
					deterministic: true,
					ignore: true,
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
				arg: arg.clone(),
				artifact: artifact.clone(),
				root_artifact: artifact.clone(),
				cache_path: cache_path.to_owned(),
				existing_artifact: existing_artifact.clone(),
				files: files.clone(),
				temp_path: path.clone(),
				final_path: path.clone(),
				root_path: path.clone(),
				progress: progress.clone(),
				visited: visited.clone(),
			};
			Box::pin(self.check_out_inner(arg)).await?;

			// Create the output.
			let output = tg::artifact::checkout::Output { path };

			Ok(output)
		} else {
			// Get the path in the cache path.
			let id = artifact.id(self).await?;
			let path = cache_path.join(id.to_string());
			let artifact_path = self.artifacts_path().join(id.to_string());

			// If there is already a file system object at the path, then return.
			if tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, %path = path.display(), "failed to stat the path"))?
			{
				let output = tg::artifact::checkout::Output {
					path: artifact_path,
				};
				return Ok(output);
			}

			// Create a temp.
			let temp = Temp::new(self);

			// Perform the checkout to the temp.
			let files = Arc::new(DashMap::default());
			let arg = InnerArg {
				arg: arg.clone(),
				artifact: artifact.clone(),
				root_artifact: artifact.clone(),
				cache_path: cache_path.to_owned(),
				existing_artifact: None,
				files,
				temp_path: temp.path.clone(),
				final_path: path.clone(),
				root_path: path.clone(),
				progress: progress.clone(),
				visited: visited.clone(),
			};
			Box::pin(self.check_out_inner(arg)).await?;
			if let Some(parent) = path.parent() {
				tokio::fs::create_dir_all(parent)
					.await
					.map_err(|source| tg::error!(!source, "failed to create output directory"))?;
			}

			// Move the checkout to the cache path.
			match tokio::fs::rename(&temp.path, &path).await {
				Ok(()) => (),

				// If the entry in the cache path exists, then remove the checkout to the temp.
				Err(ref error)
					if matches!(error.raw_os_error(), Some(libc::ENOTEMPTY | libc::EEXIST)) =>
				{
					crate::util::fs::remove(&temp.path).await.ok();
				},

				// Otherwise, return the error.
				Err(source) => {
					return Err(
						tg::error!(!source, %temp = temp.path.display(), %path = path.display(), "failed to move the checkout to the cache directory"),
					);
				},
			};

			// Create the output.
			let output = tg::artifact::checkout::Output {
				path: artifact_path,
			};

			Ok(output)
		}
	}

	async fn check_out_inner(&self, arg: InnerArg) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			root_artifact,
			cache_path,
			existing_artifact,
			files,
			temp_path,
			final_path,
			root_path,
			progress,
			visited,
		} = arg;

		// Check if this artifact has already been checked out by this task to avoid cycles.
		if !visited.insert(final_path.clone()) {
			return Ok(());
		}

		// If the artifact is the same as the existing artifact, then return.
		let id = artifact.id(self).await?;
		match &existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if id == existing_artifact.id(self).await? {
					return Ok(());
				}
			},
		}

		// Call the appropriate function for the artifact's type.
		let arg_ = InnerArg {
			arg: arg.clone(),
			artifact: artifact.clone(),
			root_artifact,
			cache_path,
			existing_artifact,
			files,
			temp_path: temp_path.clone(),
			final_path,
			root_path,
			progress,
			visited,
		};
		match artifact {
			tg::Artifact::Directory(_) => {
				Box::pin(self.check_out_directory(&arg_)).await.map_err(
					|source| tg::error!(!source, %id, %path = temp_path.display(), "failed to check out the directory"),
				)?;
			},

			tg::Artifact::File(_) => {
				Box::pin(self.check_out_file(&arg_)).await.map_err(
					|source| tg::error!(!source, %id, %path = temp_path.display(), "failed to check out the file"),
				)?;
			},

			tg::Artifact::Symlink(_) => {
				Box::pin(self.check_out_symlink(&arg_)).await.map_err(
					|source| tg::error!(!source, %id, %path = temp_path.display(), "failed to check out the symlink"),
				)?;
			},
		}

		// If this is an internal checkout, then set the file system object's modified time to the epoch.
		if arg.path.is_none() {
			tokio::task::spawn_blocking({
				let path = temp_path.clone();
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

		Ok(())
	}

	async fn check_out_directory(&self, arg: &InnerArg) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			root_artifact,
			cache_path,
			existing_artifact,
			files,
			temp_path,
			final_path,
			root_path,
			progress,
			visited,
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
							let entry_path = temp_path.clone().join(name);
							crate::util::fs::remove(&entry_path).await.ok();
						}
						Ok::<_, tg::Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await?;
			},

			// If there is an existing file system object at the path and it is not a directory, then remove it, create a directory, and continue.
			Some(_) => {
				crate::util::fs::remove(temp_path).await.ok();
				tokio::fs::create_dir_all(temp_path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},

			// If there is no artifact at this path, then create a directory.
			None => {
				tokio::fs::create_dir_all(temp_path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},
		}

		// Recurse into the entries.
		#[allow(clippy::manual_async_fn)]
		fn future(server: Server, arg: InnerArg) -> impl Future<Output = tg::Result<()>> + Send {
			async move { server.check_out_inner(arg).await }
		}

		directory
			.entries(self)
			.await?
			.iter()
			.map(|(name, artifact)| {
				let existing_artifact = &existing_artifact;
				let files = files.clone();
				let visited = visited.clone();
				async move {
					// Retrieve an existing artifact.
					let existing_artifact = match existing_artifact {
						Some(tg::Artifact::Directory(existing_directory)) => {
							existing_directory.try_get(self, name).await?
						},
						_ => None,
					};

					// Recurse.
					let temp_path = temp_path.clone().join(name);
					let final_path = final_path.clone().join(name);
					let arg = InnerArg {
						arg: arg.clone(),
						artifact: artifact.clone(),
						root_artifact: root_artifact.clone(),
						cache_path: cache_path.clone(),
						existing_artifact: existing_artifact.clone(),
						files,
						temp_path,
						final_path,
						root_path: root_path.clone(),
						progress: progress.clone(),
						visited,
					};
					tokio::spawn(future(self.clone(), arg))
						.map(|result| result.unwrap())
						.await?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		Ok(())
	}

	async fn check_out_file(&self, arg: &InnerArg) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			cache_path,
			existing_artifact,
			files,
			temp_path,
			progress,
			visited,
			..
		} = arg;

		let file = artifact
			.try_unwrap_file_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a file"))?;

		// Handle an existing artifact at the path.
		if existing_artifact.is_some() {
			crate::util::fs::remove(temp_path).await.ok();
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
					// Don't recurse on artifacts that have already been checked out.
					if visited.contains(&cache_path.join(artifact.to_string())) {
						return Ok(());
					}
					let arg = tg::artifact::checkout::Arg {
						force: false,
						path: None,
						dependencies: true,
					};
					Box::pin(self.check_out_artifact_with_files(
						artifact,
						&arg,
						cache_path,
						files.clone(),
						visited.clone(),
						progress,
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
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			tokio::fs::copy(&existing_path, &temp_path).await.map_err(
				|source| tg::error!(!source, %existing_path = temp_path.display(), %to = temp_path.display(), %id, "failed to copy the file"),
			)?;
			return Ok(());
		}

		// Attempt to use the file from an internal checkout.
		let internal_checkout_path = cache_path.join(id.to_string());
		if arg.path.is_none() {
			// If this checkout is internal, then create a hard link.
			let result = tokio::fs::hard_link(&internal_checkout_path, temp_path).await;
			if result.is_ok() {
				return Ok(());
			}
		} else {
			// If this checkout is external, then copy the file.
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			let result = tokio::fs::copy(&internal_checkout_path, temp_path).await;
			if result.is_ok() {
				return Ok(());
			}
		}

		// Create the file.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		tokio::io::copy(
			&mut file.reader(self).await?,
			&mut tokio::fs::File::create(temp_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the file"))?,
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to write the bytes"))?;
		drop(permit);

		// Make the file executable if necessary.
		if file.executable(self).await? {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(temp_path, permissions)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		// Set the extended attributes.
		let name = tg::file::XATTR_DATA_NAME;
		let xattr = file.data(self).await?;
		let json = serde_json::to_vec(&xattr)
			.map_err(|error| tg::error!(source = error, "failed to serialize the dependencies"))?;
		xattr::set(temp_path, name, &json).map_err(|source| {
			tg::error!(!source, "failed to set the extended attribute for the file")
		})?;

		// Add the path to the files map.
		files.insert(id.clone(), temp_path.clone());

		Ok(())
	}

	async fn check_out_symlink(&self, arg: &InnerArg) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			root_artifact,
			cache_path,
			existing_artifact,
			files,
			temp_path,
			final_path,
			root_path,
			progress,
			visited,
		} = arg;

		let symlink = artifact
			.try_unwrap_symlink_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a symlink"))?;

		// Handle an existing artifact at the path.
		if existing_artifact.is_some() {
			crate::util::fs::remove(&temp_path).await.ok();
		};

		// Get the symlink's data.
		let artifact = symlink.artifact(self).await?;
		let subpath = symlink.subpath(self).await?;

		// Fail if the symlink is garbage.
		if artifact.is_none() && subpath.is_none() {
			return Err(tg::error!("invalid symlink"));
		}

		// Check out the symlink's artifact if necessary.
		if arg.dependencies {
			if let Some(artifact) = &artifact {
				let target_id = artifact.id(self).await?;

				// Don't recurse on artifacts that are already checked out.
				if !visited.contains(&cache_path.join(target_id.to_string())) {
					let arg = tg::artifact::checkout::Arg::default();
					Box::pin(self.check_out_artifact_with_files(
						&target_id,
						&arg,
						cache_path,
						files.clone(),
						visited.clone(),
						progress,
					))
					.await?;
				}
			}
		}

		// Render the target.
		let mut target: PathBuf = PathBuf::new();
		if let Some(artifact) = &artifact {
			// Get the artifact IDs.
			let target_id = artifact.id(self).await?;
			let root_id = root_artifact.id(self).await?;
			let link_id = symlink.id(self).await?.into();

			if root_id == link_id || root_id != target_id {
				// If this symlink is the root item or the target artifact is different from the root, write a symlink relative to the cache directory.
				let diff =
					crate::util::path::diff(final_path.parent().unwrap(), cache_path).unwrap();
				target.push(diff.join(target_id.to_string()));
			} else {
				// Otherwise, write a relative path to the root.
				let diff =
					crate::util::path::diff(final_path.parent().unwrap(), root_path).unwrap();
				target.push(diff);
			}
		}
		if let Some(subpath) = subpath {
			target.push(subpath);
		}

		// Create the symlink.
		tokio::fs::symlink(&target, &temp_path)
			.await
			.map_err(|source| tg::error!(!source, %src = target.display(), %dst = temp_path.display(), "failed to create the symlink"))?;

		Ok(())
	}
}

impl Server {
	#[allow(clippy::unused_self, clippy::unused_async)]
	async fn get_cache_path_for_path(
		&self,
		artifact: &tg::artifact::Id,
		output_path: Option<&Path>,
	) -> tg::Result<PathBuf> {
		let Some(output_path) = output_path else {
			return Ok(self.cache_path());
		};

		if !output_path.is_absolute() {
			return Err(tg::error!(%path = output_path.display(), "expected an absolute path"));
		}

		let cache_path = if let tg::artifact::Id::Directory(_) = artifact {
			output_path.join(".tangram/artifacts")
		} else {
			output_path.parent().unwrap().join(".tangram/artifacts")
		};

		Ok(cache_path)
	}

	async fn create_lockfile_with_artifact(
		&self,
		artifact: &tg::Artifact,
	) -> tg::Result<tg::Lockfile> {
		// Create the state.
		let mut nodes = Vec::new();
		let mut visited = BTreeMap::new();
		let mut graphs = BTreeMap::new();

		// Create nodes in the lockfile for the graph.
		self.get_or_create_lockfile_node_with_artifact(
			artifact,
			&mut nodes,
			&mut visited,
			&mut graphs,
		)
		.await?;

		// Strip nodes.
		let nodes: Vec<_> = nodes
			.into_iter()
			.enumerate()
			.map(|(index, node)| {
				node.ok_or_else(
					|| tg::error!(%node = index, "invalid graph, failed to create lockfile node"),
				)
			})
			.try_collect()?;
		let nodes = self.strip_lockfile_nodes(&nodes, 0)?;

		// Create the lockfile.
		Ok(tg::Lockfile { nodes })
	}

	async fn get_or_create_lockfile_node_with_artifact(
		&self,
		artifact: &tg::Artifact,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<tg::artifact::Id, usize>,
		graphs: &mut BTreeMap<tg::graph::Id, Vec<usize>>,
	) -> tg::Result<usize> {
		let id = artifact.id(self).await?;
		if let Some(visited) = visited.get(&id) {
			return Ok(*visited);
		}
		let index = nodes.len();

		// Flatten graphs into the lockfile.
		'a: {
			let (graph, node) = match artifact.object(self).await? {
				tg::artifact::Object::Directory(directory) => {
					let tg::directory::Object::Graph { graph, node } = directory.as_ref() else {
						break 'a;
					};
					(graph.clone(), *node)
				},
				tg::artifact::Object::File(file) => {
					let tg::file::Object::Graph { graph, node } = file.as_ref() else {
						break 'a;
					};
					(graph.clone(), *node)
				},
				tg::artifact::Object::Symlink(symlink) => {
					let tg::symlink::Object::Graph { graph, node } = symlink.as_ref() else {
						break 'a;
					};
					(graph.clone(), *node)
				},
			};
			let nodes =
				Box::pin(self.create_lockfile_node_with_graph(&graph, nodes, visited, graphs))
					.await?;
			return Ok(nodes[node]);
		}

		// Only create a distinct node for non-graph artifacts.
		nodes.push(None);
		visited.insert(id, index);

		// Create a new lockfile node for the artifact, recursing over dependencies.
		let node = match artifact.object(self).await? {
			tg::artifact::Object::Directory(directory) => {
				let tg::directory::Object::Normal { entries } = directory.as_ref() else {
					unreachable!()
				};
				let mut entries_ = BTreeMap::new();
				for (name, artifact) in entries {
					let index = Box::pin(self.get_or_create_lockfile_node_with_artifact(
						artifact, nodes, visited, graphs,
					))
					.await?;
					entries_.insert(name.clone(), Either::Left(index));
				}
				tg::lockfile::Node::Directory { entries: entries_ }
			},

			tg::artifact::Object::File(file) => {
				let tg::file::Object::Normal {
					contents,
					dependencies,
					executable,
				} = file.as_ref()
				else {
					unreachable!()
				};
				let mut dependencies_ = BTreeMap::new();
				for (reference, referent) in dependencies {
					let item = match &referent.item {
						tg::Object::Directory(directory) => {
							let artifact = directory.clone().into();
							let index = Box::pin(self.get_or_create_lockfile_node_with_artifact(
								&artifact, nodes, visited, graphs,
							))
							.await?;
							Either::Left(index)
						},
						tg::Object::File(file) => {
							let artifact = file.clone().into();
							let index = Box::pin(self.get_or_create_lockfile_node_with_artifact(
								&artifact, nodes, visited, graphs,
							))
							.await?;
							Either::Left(index)
						},
						tg::Object::Symlink(symlink) => {
							let artifact = symlink.clone().into();
							let index = Box::pin(self.get_or_create_lockfile_node_with_artifact(
								&artifact, nodes, visited, graphs,
							))
							.await?;
							Either::Left(index)
						},
						object => Either::Right(object.id(self).await?),
					};
					let dependency = tg::Referent {
						item,
						path: referent.path.clone(),
						subpath: referent.subpath.clone(),
						tag: referent.tag.clone(),
					};
					dependencies_.insert(reference.clone(), dependency);
				}
				let contents = Some(contents.id(self).await?);
				tg::lockfile::Node::File {
					contents,
					dependencies: dependencies_,
					executable: *executable,
				}
			},

			tg::artifact::Object::Symlink(symlink) => {
				let tg::symlink::Object::Normal { artifact, subpath } = symlink.as_ref() else {
					unreachable!();
				};
				let artifact = if let Some(artifact) = artifact {
					let index = Box::pin(self.get_or_create_lockfile_node_with_artifact(
						artifact, nodes, visited, graphs,
					))
					.await?;
					Some(Either::Left(index))
				} else {
					None
				};
				let subpath = subpath.as_ref().map(PathBuf::from);
				tg::lockfile::Node::Symlink { artifact, subpath }
			},
		};

		// Update the visited set.
		nodes[index].replace(node);

		Ok(index)
	}

	async fn create_lockfile_node_with_graph(
		&self,
		graph: &tg::Graph,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<tg::artifact::Id, usize>,
		graphs: &mut BTreeMap<tg::graph::Id, Vec<usize>>,
	) -> tg::Result<Vec<usize>> {
		let id = graph.id(self).await?;
		if let Some(existing) = graphs.get(&id) {
			return Ok(existing.clone());
		}

		// Get the graph object.
		let object = graph.object(self).await?;

		// Assign indices.
		let mut indices = Vec::with_capacity(object.nodes.len());
		for node in 0..object.nodes.len() {
			let id = match object.nodes[node].kind() {
				tg::artifact::Kind::Directory => {
					tg::Directory::with_graph_and_node(graph.clone(), node)
						.id(self)
						.await?
						.into()
				},
				tg::artifact::Kind::File => tg::Directory::with_graph_and_node(graph.clone(), node)
					.id(self)
					.await?
					.into(),
				tg::artifact::Kind::Symlink => {
					tg::Directory::with_graph_and_node(graph.clone(), node)
						.id(self)
						.await?
						.into()
				},
			};

			let index = visited.get(&id).copied().unwrap_or_else(|| {
				let index = nodes.len();
				visited.insert(id, index);
				nodes.push(None);
				index
			});
			indices.push(index);
		}
		graphs.insert(id.clone(), indices.clone());

		// Create nodes
		for (old_index, node) in object.nodes.iter().enumerate() {
			let node = match node {
				tg::graph::Node::Directory(directory) => {
					let mut entries = BTreeMap::new();
					for (name, entry) in &directory.entries {
						let index = match entry {
							Either::Left(index) => indices[*index],
							Either::Right(artifact) => {
								Box::pin(self.get_or_create_lockfile_node_with_artifact(
									artifact, nodes, visited, graphs,
								))
								.await?
							},
						};
						entries.insert(name.clone(), Either::Left(index));
					}
					tg::lockfile::Node::Directory { entries }
				},

				tg::graph::Node::File(file) => {
					let mut dependencies = BTreeMap::new();
					for (reference, referent) in &file.dependencies {
						let item = match &referent.item {
							Either::Left(index) => Either::Left(indices[*index]),
							Either::Right(object) => match object {
								tg::Object::Directory(artifact) => {
									let artifact = artifact.clone().into();
									let index =
										Box::pin(self.get_or_create_lockfile_node_with_artifact(
											&artifact, nodes, visited, graphs,
										))
										.await?;
									Either::Left(index)
								},
								tg::Object::File(artifact) => {
									let artifact = artifact.clone().into();
									let index =
										Box::pin(self.get_or_create_lockfile_node_with_artifact(
											&artifact, nodes, visited, graphs,
										))
										.await?;
									Either::Left(index)
								},
								tg::Object::Symlink(artifact) => {
									let artifact = artifact.clone().into();
									let index =
										Box::pin(self.get_or_create_lockfile_node_with_artifact(
											&artifact, nodes, visited, graphs,
										))
										.await?;
									Either::Left(index)
								},
								object => Either::Right(object.id(self).await?),
							},
						};
						let path = referent.path.clone();
						let subpath = referent.subpath.clone();
						let tag = referent.tag.clone();
						let dependency = tg::Referent {
							item,
							path,
							subpath,
							tag,
						};
						dependencies.insert(reference.clone(), dependency);
					}
					let contents = file.contents.id(self).await?;
					let executable = file.executable;
					tg::lockfile::Node::File {
						contents: Some(contents),
						dependencies,
						executable,
					}
				},

				tg::graph::Node::Symlink(symlink) => {
					let artifact = match &symlink.artifact {
						Some(Either::Left(index)) => Some(indices[*index]),
						Some(Either::Right(artifact)) => {
							let index = Box::pin(self.get_or_create_lockfile_node_with_artifact(
								artifact, nodes, visited, graphs,
							))
							.await?;
							Some(index)
						},
						None => None,
					};
					let artifact = artifact.map(Either::Left);
					let subpath = symlink.subpath.clone().map(PathBuf::from);
					tg::lockfile::Node::Symlink { artifact, subpath }
				},
			};
			let index = indices[old_index];
			nodes[index].replace(node);
		}

		Ok(indices)
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
		// Parse the ID.
		let id = id.parse()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.check_out_artifact(&id, arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None => {
				pin!(stream)
					.try_last()
					.await?
					.and_then(|event| event.try_unwrap_output().ok())
					.ok_or_else(|| tg::error!("stream ended without output"))?;
				(None, Outgoing::empty())
			},

			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Outgoing::sse(stream))
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
