use crate::{temp::Temp, Server};
use dashmap::{DashMap, DashSet};
use futures::{stream::FuturesUnordered, FutureExt, Stream, StreamExt as _, TryStreamExt as _};
use itertools::Itertools;
use std::{
	collections::BTreeMap,
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

#[derive(Clone, Debug)]
struct InnerArg {
	// An existing artifact at the destination path.
	existing_artifact: Option<tg::Artifact>,

	// The path to move to.
	final_path: PathBuf,

	// The current artifact being checked out.
	new_artifact: tg::artifact::Id,
}

#[derive(Clone, Debug)]
struct State {
	// The top level checkout arg.
	arg: tg::artifact::checkout::Arg,

	// The root artifact being checked out.
	artifact: tg::artifact::Id,

	// The path to the current cache directory, either internal or external.
	cache_path: PathBuf,

	// Set of visited files and their paths.
	files: Arc<DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>>,

	// Current progress.
	_progress: crate::progress::Handle<tg::artifact::checkout::Output>,

	// Set of visited paths.
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
		// Create the progress handle.
		let progress = crate::progress::Handle::new();

		// Spawn the task.
		let task = tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let progress = progress.clone();
			async move { server.check_out_artifact_task(id, arg, &progress).await }
		});

		// Spawn the progress task.
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

		// Create the stream.
		let stream = progress.stream();
		Ok(stream)
	}

	async fn check_out_artifact_task(
		&self,
		artifact: tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
		// Get or spawn the task.
		let spawn = |_| {
			let server = self.clone();
			let arg = arg.clone();
			let artifact = artifact.clone();
			let files = Arc::new(DashMap::default());
			let progress = progress.clone();
			let visited = Arc::new(DashSet::default());

			async move {
				// Compute the cache path.
				let cache_path = server
					.get_cache_path_for_path(&artifact, arg.path.as_deref())
					.await?;

				// Create the arg.
				let final_path = arg
					.path
					.clone()
					.unwrap_or_else(|| cache_path.join(artifact.to_string()));
				let inner_arg = InnerArg {
					existing_artifact: None,
					final_path: arg
						.path
						.clone()
						.unwrap_or_else(|| cache_path.join(artifact.to_string())),
					new_artifact: artifact.clone(),
				};

				// Create the state.
				let state = State {
					arg: arg.clone(),
					artifact: artifact.clone(),
					cache_path,
					files,
					_progress: progress,
					visited,
				};

				// Perform the checkout.
				server.check_out_artifact_inner(inner_arg, state).await?;

				// If this is an external checkout, create a lockfile and write it if non-empty.
				if let Some(path) = arg.path {
					let artifact = tg::Artifact::with_id(artifact.clone());
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

				Ok(tg::artifact::checkout::Output { path: final_path })
			}
		};
		let internal = arg.path.is_none();
		let task = if internal {
			self.checkout_task_map.get_or_spawn(artifact.clone(), spawn)
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

	async fn check_out_artifact_inner(&self, arg: InnerArg, state: State) -> tg::Result<()> {
		let InnerArg {
			existing_artifact,
			final_path,
			new_artifact,
		} = arg;

		// Check if we've already visited this item.
		if !state.visited.insert(final_path.clone()) {
			return Ok(());
		}

		// Create a temp and compute the dest path.
		let temp = Temp::new(self);
		let dest_path = if final_path.starts_with(self.cache_path()) {
			temp.path.clone()
		} else {
			final_path.clone()
		};

		// Check if the destination exists.
		let exists = tokio::fs::try_exists(&final_path).await.unwrap_or(false);

		// Skip duplicate checkouts to the cache.
		if exists && final_path.starts_with(&state.cache_path) {
			return Ok(());
		}

		// If the destination exists and this isn't a forced checkout, return an error.
		if exists && !state.arg.force {
			return Err(
				tg::error!(%path = final_path.display(), "there is already a file system object at the path"),
			);
		}

		// If the object exists, try and perform a checkin.
		let existing_artifact = if exists && existing_artifact.is_none() {
			let arg = tg::artifact::checkin::Arg {
				destructive: false,
				deterministic: true,
				ignore: true,
				locked: true,
				path: final_path.clone(),
			};
			let artifact = tg::Artifact::check_in(self, arg).await
				.map_err(|source| tg::error!(!source, %path = final_path.display(), "failed to check in existing artifact"))?;
			Some(artifact)
		} else {
			// Reuse the artifact passed as an arg.
			existing_artifact
		};

		// Perform the checkout.
		match new_artifact {
			tg::artifact::Id::Directory(artifact) => {
				self.check_out_directory(artifact, &dest_path, existing_artifact, state)
					.await?;
			},
			tg::artifact::Id::File(artifact) => {
				self.check_out_file(artifact, &dest_path, existing_artifact, state)
					.await?;
			},
			tg::artifact::Id::Symlink(artifact) => {
				self.check_out_symlink(artifact, &dest_path, &final_path, existing_artifact, state)
					.await?;
			},
		};

		// There is no additional work to do if the dest/final paths are the same.
		if dest_path == final_path {
			return Ok(());
		}

		// Rename if necessary.
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		match tokio::fs::rename(&dest_path, &final_path).await {
			Ok(()) => (),
			Err(ref error)
				if matches!(error.raw_os_error(), Some(libc::ENOTEMPTY | libc::EEXIST)) => {},
			Err(source) => {
				return Err(
					tg::error!(!source, %src = dest_path.display(), %dst = final_path.display(), "failed to rename artifact"),
				)
			},
		}

		Ok(())
	}

	async fn check_out_directory(
		&self,
		directory: tg::directory::Id,
		dest: &Path,
		existing_artifact: Option<tg::Artifact>,
		state: State,
	) -> tg::Result<()> {
		// Create the directory handle.
		let directory = tg::Directory::with_id(directory);

		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing directory, then remove any extraneous entries.
			Some(tg::Artifact::Directory(existing_directory)) => {
				existing_directory
					.entries(self)
					.await?
					.keys()
					.map(|name| {
						let directory = directory.clone();
						let dest = dest.to_owned();
						async move {
							if !directory.entries(self).await?.contains_key(name) {
								let entry_path = dest.join(name);
								crate::util::fs::remove(&entry_path).await.ok();
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
				crate::util::fs::remove(dest).await.ok();
				tokio::fs::create_dir_all(dest)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},

			// If there is no artifact at this path, then create a directory.
			None => {
				tokio::fs::create_dir_all(dest)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			},
		}

		// Recurse into the entries.
		#[allow(clippy::manual_async_fn)]
		fn future(
			server: Server,
			arg: InnerArg,
			state: State,
		) -> impl Future<Output = tg::Result<()>> + Send + 'static {
			async move { server.check_out_artifact_inner(arg, state).await }
		}

		// Check out children.
		directory
			.entries(self)
			.await?
			.into_iter()
			.map(|(name, artifact)| {
				let server = self.clone();
				let state = state.clone();
				let existing_artifact = existing_artifact.clone();
				async move {
					let existing_artifact =
						if let Some(tg::Artifact::Directory(existing_directory)) =
							&existing_artifact
						{
							existing_directory.try_get_entry(&server, &name).await?
						} else {
							None
						};
					let final_path = dest.join(&name);
					let new_artifact = artifact.id(&server).await?;
					let arg = InnerArg {
						existing_artifact,
						final_path,
						new_artifact,
					};
					tokio::spawn(future(self.clone(), arg, state.clone()))
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

	async fn check_out_file(
		&self,
		artifact: tg::file::Id,
		dest: &Path,
		existing_artifact: Option<tg::Artifact>,
		state: State,
	) -> tg::Result<()> {
		let file = tg::File::with_id(artifact.clone());

		// Handle an existing artifact at the path.
		if existing_artifact.is_some() {
			crate::util::fs::remove(dest).await.ok();
		};

		// Check out the file's dependencies.
		if state.arg.dependencies {
			let dependencies = file
				.dependencies(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?
				.values()
				.cloned()
				.collect::<Vec<_>>();

			#[allow(clippy::manual_async_fn)]
			fn future(
				server: Server,
				referent: tg::Referent<tg::Object>,
				state: State,
			) -> impl Future<Output = tg::Result<()>> + Send + 'static {
				async move {
					// Skip object dependencies.
					let new_artifact: tg::Artifact = match referent.item {
						tg::Object::Directory(directory) => directory.into(),
						tg::Object::File(file) => file.into(),
						tg::Object::Symlink(symlink) => symlink.into(),
						_ => return Ok(()),
					};
					let new_artifact = new_artifact.id(&server).await?;

					// Skip checkouts within the same root.
					if new_artifact == state.artifact {
						return Ok(());
					}

					// Create a new arg
					let final_path = state.cache_path.join(new_artifact.to_string());
					let arg = InnerArg {
						existing_artifact: None,
						final_path,
						new_artifact: new_artifact.clone(),
					};

					// Create a new state.
					let state = State {
						arg: tg::artifact::checkout::Arg {
							path: None,
							..state.arg.clone()
						},
						artifact: new_artifact,
						..state.clone()
					};

					// Check out the dependency.
					server.check_out_artifact_inner(arg, state).await
				}
			}
			dependencies
				.into_iter()
				.map(|referent| {
					let server = self.clone();
					let state = state.clone();
					async move { tokio::spawn(future(server, referent, state)).await.unwrap() }
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<()>()
				.await?;
		}

		// Attempt to copy the file from another file in the checkout.
		let existing_path = state.files.get(&artifact).map(|path| path.clone());
		if let Some(existing_path) = existing_path {
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			tokio::fs::copy(&existing_path, dest).await.map_err(
				|source| tg::error!(!source, %existing_path = existing_path.display(), %to = dest.display(), %artifact, "failed to copy the file"),
			)?;
			return Ok(());
		}

		// Attempt to use the file from an internal checkout.
		let internal_checkout_path = state.cache_path.join(artifact.to_string());
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let result = tokio::fs::copy(&internal_checkout_path, dest).await;
		drop(permit);
		if result.is_ok() {
			return Ok(());
		}

		// Create the file.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		tokio::io::copy(
			&mut file.reader(self).await?,
			&mut tokio::fs::File::create(dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the file"))?,
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to write the bytes"))?;
		drop(permit);

		// Make the file executable if necessary.
		if file.executable(self).await? {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(dest, permissions)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}

		// Set the extended attributes.
		let name = tg::file::XATTR_DATA_NAME;
		let xattr = file.data(self).await?;
		let json = serde_json::to_vec(&xattr)
			.map_err(|error| tg::error!(source = error, "failed to serialize the dependencies"))?;
		xattr::set(dest, name, &json).map_err(|source| {
			tg::error!(!source, "failed to set the extended attribute for the file")
		})?;

		// Add the path to the files map.
		if let dashmap::Entry::Vacant(vacant) = state.files.entry(artifact.clone()) {
			vacant.insert(dest.to_owned());
		}

		Ok(())
	}

	async fn check_out_symlink(
		&self,
		artifact: tg::symlink::Id,
		dest: &Path,
		final_path: &Path,
		existing_artifact: Option<tg::Artifact>,
		state: State,
	) -> tg::Result<()> {
		let symlink = tg::Symlink::with_id(artifact.clone());

		// Handle an existing artifact at the path.
		if existing_artifact.is_some() {
			crate::util::fs::remove(dest).await.ok();
		};

		// Get the symlink's data.
		let target_artifact = symlink.artifact(self).await?;
		let subpath = symlink.subpath(self).await?;

		// Fail if the symlink is invalid.
		if target_artifact.is_none() && subpath.is_none() {
			return Err(tg::error!(
				"invalid symlink, expected an artifact and/or a subpath"
			));
		}

		// Check out the symlink's artifact if necessary.
		if state.arg.dependencies {
			if let Some(new_artifact) = &target_artifact {
				let new_artifact = new_artifact.id(self).await?;

				// If this is an external symlink, check out its artifact.
				if new_artifact != state.artifact {
					// Create a new arg.
					let final_path = state.cache_path.join(new_artifact.to_string());
					let arg = InnerArg {
						existing_artifact: None,
						final_path,
						new_artifact: new_artifact.clone(),
					};

					// Create a new state.
					let state = State {
						arg: tg::artifact::checkout::Arg {
							path: None,
							..state.arg.clone()
						},
						artifact: new_artifact,
						..state.clone()
					};

					// Spawn instead of Box to avoid stack overflow.
					#[allow(clippy::manual_async_fn)]
					fn future(
						server: Server,
						arg: InnerArg,
						state: State,
					) -> impl Future<Output = tg::Result<()>> + Send + 'static {
						async move { server.check_out_artifact_inner(arg, state).await }
					}

					tokio::spawn(future(self.clone(), arg, state))
						.await
						.unwrap()?;
				}
			}
		}

		// Render the target.
		let mut target: PathBuf = PathBuf::new();
		if let Some(target_artifact) = &target_artifact {
			// Get the artifact IDs.
			let target_id = target_artifact.id(self).await?;

			if state.artifact == artifact.into() || state.artifact != target_id {
				// If this symlink is the root item or the target artifact is different from the root, write a symlink relative to the cache directory.
				let diff = crate::util::path::diff(final_path.parent().unwrap(), &state.cache_path)
					.unwrap();
				target.push(diff.join(target_id.to_string()));
			} else {
				// Otherwise, write a relative path to the root.
				let root_path = state
					.arg
					.path
					.clone()
					.unwrap_or_else(|| state.cache_path.join(target_id.to_string()));

				let diff =
					crate::util::path::diff(final_path.parent().unwrap(), &root_path).unwrap();
				target.push(diff);
			}
		}
		if let Some(subpath) = subpath {
			target.push(subpath);
		}

		// Create the symlink.
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		tokio::fs::symlink(&target, &dest)
			.await
			.map_err(|source| tg::error!(!source, %src = target.display(), %dst = dest.display(), "failed to create the symlink"))?;

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
