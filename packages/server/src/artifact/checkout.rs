use crate::{tmp::Tmp, Server};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, Stream, StreamExt as _, TryStreamExt as _};
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	pin::pin,
	sync::Arc,
};
use tangram_client as tg;
use tangram_futures::{stream::TryStreamExt as _, task::Task};
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tg::path::Ext as _;

struct InnerArg<'a> {
	arg: &'a tg::artifact::checkout::Arg,
	artifact: &'a tg::Artifact,
	checkouts_path: &'a PathBuf,
	existing_artifact: Option<&'a tg::Artifact>,
	files: Arc<DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>>,
	path: &'a PathBuf,
	progress: &'a crate::progress::Handle<tg::artifact::checkout::Output>,
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
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let progress = progress.clone();
			async move {
				let result = server.check_out_artifact_task(&id, arg, &progress).await;
				match result {
					Ok(output) => progress.output(output),
					Err(error) => progress.error(error),
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
			let progress = progress.clone();

			async move {
				let checkouts_path = server
					.get_checkouts_directory_for_path(&id, arg.path.as_deref())
					.await?;
				server
					.check_out_artifact_with_files(&id, &arg, &checkouts_path, files, &progress)
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
		checkouts_path: &PathBuf,
		files: Arc<DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>>,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
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
			if (path.as_ref() as &Path).starts_with(self.checkouts_path()) {
				return Err(
					tg::error!(%path = path.display(), "cannot check out an artifact to the checkouts directory"),
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
				arg,
				artifact: &artifact,
				checkouts_path,
				existing_artifact: existing_artifact.as_ref(),
				files,
				path: &path,
				progress,
			};
			self.check_out_inner(arg).await?;

			// Create the output.
			let output = tg::artifact::checkout::Output { path };

			Ok(output)
		} else {
			// Get the path in the checkouts directory.
			let id = artifact.id(self).await?;
			let path = checkouts_path.join(id.to_string());
			let artifact_path = self.artifacts_path().join(id.to_string());

			// If there is already a file system object at the path, then return.
			if tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to stat the path"))?
			{
				let output = tg::artifact::checkout::Output {
					path: artifact_path,
				};
				return Ok(output);
			}

			// Create a tmp.
			let tmp = Tmp::new(self);

			// Perform the checkout to the tmp.
			let files = Arc::new(DashMap::default());
			let arg = InnerArg {
				arg,
				artifact: &artifact,
				checkouts_path,
				existing_artifact: None,
				files,
				path: &tmp.path,
				progress,
			};
			self.check_out_inner(arg).await?;

			if let Some(parent) = path.parent() {
				tokio::fs::create_dir_all(parent)
					.await
					.map_err(|source| tg::error!(!source, "failed to create output directory"))?;
			}

			// Move the checkout to the checkouts directory.
			match tokio::fs::rename(&tmp, &path).await {
				Ok(()) => (),
				// If the entry in the checkouts directory exists, then remove the checkout to the tmp.
				Err(ref error)
					if matches!(error.raw_os_error(), Some(libc::ENOTEMPTY | libc::EEXIST)) =>
				{
					crate::util::fs::remove(&tmp).await.ok();
				},
				// If the checkout is on a different filesystem, perform a deep copy.
				Err(ref error) if matches!(error.raw_os_error(), Some(libc::ENOSYS)) => {
					todo!()
				},

				// Otherwise, return the error.
				Err(source) => {
					return Err(
						tg::error!(!source, %tmp = tmp.path.display(), %path = path.display(), "failed to move the checkout to the checkouts directory"),
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

	async fn check_out_inner(&self, arg: InnerArg<'_>) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			checkouts_path,
			existing_artifact,
			files,
			path,
			progress,
		} = arg;

		// If the artifact is the same as the existing artifact, then return.
		let id = artifact.id(self).await?;
		match existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if id == existing_artifact.id(self).await? {
					return Ok(());
				}
			},
		}

		// Call the appropriate function for the artifact's type.
		let arg_ = InnerArg {
			arg,
			artifact,
			checkouts_path,
			existing_artifact,
			files,
			path,
			progress,
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

		Ok(())
	}

	async fn check_out_directory(&self, arg: InnerArg<'_>) -> tg::Result<()> {
		let InnerArg {
			arg,
			artifact,
			checkouts_path,
			existing_artifact,
			files,
			path,
			progress,
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
				crate::util::fs::remove(path).await.ok();
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
							existing_directory.try_get(self, name).await?
						},
						_ => None,
					};

					// Recurse.
					let entry_path = path.clone().join(name);
					let arg = InnerArg {
						arg,
						artifact,
						checkouts_path,
						existing_artifact: existing_artifact.as_ref(),
						files,
						path: &entry_path,
						progress,
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
			checkouts_path,
			existing_artifact,
			files,
			path,
			progress,
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
				crate::util::fs::remove(path).await.ok();
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

		if arg.references && !dependencies.is_empty() {
			dependencies
				.iter()
				.map(|artifact| async {
					let arg = tg::artifact::checkout::Arg {
						force: false,
						path: None,
						references: true,
					};
					Box::pin(self.check_out_artifact_with_files(
						artifact,
						&arg,
						checkouts_path,
						files.clone(),
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
			let permit = self.file_descriptor_semaphore.acquire().await;
			tokio::fs::copy(&existing_path, &path).await.map_err(
				|source| tg::error!(!source, %existing_path = path.display(), %to = path.display(), %id, "failed to copy the file"),
			)?;
			drop(permit);
			return Ok(());
		}

		// Attempt to use the file from an internal checkout.
		let internal_checkout_path = checkouts_path.join(id.to_string());
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
		let name = tg::file::XATTR_DATA_NAME;
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
			checkouts_path,
			existing_artifact,
			files,
			path,
			progress,
		} = arg;

		let symlink = artifact
			.try_unwrap_symlink_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a symlink"))?;

		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				crate::util::fs::remove(&path).await.ok();
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Get the symlink's data.
		let artifact = symlink.artifact(self).await?;
		let path_ = symlink.path(self).await?;

		// Check out the symlink's artifact if necessary.
		if arg.references {
			if let Some(artifact) = &artifact {
				let id = artifact.id(self).await?;
				let arg = tg::artifact::checkout::Arg::default();
				Box::pin(self.check_out_artifact_with_files(
					&id,
					&arg,
					checkouts_path,
					files,
					progress,
				))
				.await?;
			}
		}

		// Render the target.
		let mut target = PathBuf::new();
		if artifact.is_some() {
			target.push(path.diff(checkouts_path).unwrap().parent().unwrap());
		}
		if let Some(path) = path_ {
			target.push(path);
		}

		// Create the symlink.
		tokio::fs::symlink(target, path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;

		Ok(())
	}
}

impl Server {
	#[allow(clippy::unused_self, clippy::unused_async)]
	async fn get_checkouts_directory_for_path(
		&self,
		artifact: &tg::artifact::Id,
		output_path: Option<&Path>,
	) -> tg::Result<PathBuf> {
		let Some(output_path) = output_path else {
			return Ok(self.checkouts_path());
		};

		if !output_path.is_absolute() {
			return Err(tg::error!(%path = output_path.display(), "expected an absolute path"));
		}

		let checkouts_path = if let tg::artifact::Id::Directory(_) = artifact {
			output_path.join(".tangram/artifacts")
		} else {
			output_path.parent().unwrap().join(".tangram/artifacts")
		};

		Ok(checkouts_path)
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

impl Server {
	async fn create_lockfile_from_artifact(&self, artifact: &tg::Artifact) -> tg::Result<tg::Lockfile> {
		let mut nodes = Vec::new();
		let mut visited = BTreeMap::new();
		self.create_lockfile_from_artifact_inner(artifact, &mut nodes, &mut visited).await?;
		let nodes = nodes.into_iter().map(Option::unwrap).collect();
		Ok(tg::Lockfile { nodes })
	}

	async fn create_lockfile_from_artifact_inner(
		&self, 
		artifact: &tg::Artifact,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<tg::artifact::Id, usize>
	) -> tg::Result<usize> {
		let id = artifact.id(self).await?;
		if let Some(visited) = visited.get(&id) {
			return Ok(*visited);
		}
		let mut index = nodes.len();
		nodes.push(None);
		visited.insert(id, index);

		let node = match artifact {
			tg::Artifact::Directory(directory) => {
				todo!()
			},
			tg::Artifact::File(file) => {
				todo!()
			},
			tg::Artifact::Symlink(symlink) => {
				todo!()
			}
		};



		Ok(index)
	}

	async fn create_lockfile_nodes_from_graph(&self,
		graph: &tg::Graph,
		nodes: &mut Vec<Option<tg::lockfile::Node>>,
		visited: &mut BTreeMap<tg::artifact::Id, usize>
	) -> tg::Result<()> {
		let object = graph.object(self).await?;
		let mut indices = Vec::with_capacity(object.nodes.len());

		// Assign indices.
		for node in 0..object.nodes.len() {
			let index = nodes.len();
			nodes.push(None);
			indices.push(index);
		}

		// Create nodes
		for (old_index, node) in object.nodes.iter().enumerate() {
			match node {
				tg::graph::Node::Directory(directory) => {
					
				},
				tg::graph::Node::File(file) => {

				},
				tg::graph::Node::Symlink(symlink) => {

				},
			}
		}


		Ok(())
	}
}