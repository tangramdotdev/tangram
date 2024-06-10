use crate::{tmp::Tmp, util::fs::remove, Server};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, Stream, StreamExt as _, TryStreamExt as _};
use std::{
	collections::BTreeSet,
	os::unix::fs::PermissionsExt as _,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};
use tangram_client as tg;
use tangram_futures::task::Task;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tg::Handle;

impl Server {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkout::Event>>> {
		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn a task.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				if let Err(error) = server.check_out_task(&id, arg, sender.clone()).await {
					sender.try_send(Err(error)).ok();
				}
			}
		});

		Ok(receiver)
	}

	async fn check_out_task(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		sender: async_channel::Sender<tg::Result<tg::artifact::checkout::Event>>,
	) -> tg::Result<()> {
		// Get the total size.
		let metadata = self.get_object_metadata(&id.clone().into()).await?;
		let event = tg::artifact::checkout::Event::Progress(tg::artifact::checkout::Progress {
			count: tg::Progress {
				current: 0,
				total: metadata.count,
			},
			weight: tg::Progress {
				current: 0,
				total: metadata.weight,
			},
		});
		sender.try_send(Ok(event)).ok();

		// Get or spawn the task.
		let spawn = |_| {
			let server = self.clone();
			let id = id.clone();
			let files = Arc::new(DashMap::default());
			let arg = arg.clone();
			let sender = sender.clone();
			let count = AtomicU64::new(0);
			let weight = AtomicU64::new(0);
			async move {
				server
					.check_out_artifact_with_files(&id, &arg, files, &sender, &count, &weight)
					.await
			}
		};

		let internal = arg.path.is_none();
		let task = if internal {
			self.checkouts.get_or_spawn(id.clone(), spawn)
		} else {
			Task::spawn(spawn)
		};

		// Await the task.
		let path = task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the task failed"))??;

		// Send the end event.
		sender
			.try_send(Ok(tg::artifact::checkout::Event::End(path)))
			.ok();

		Ok(())
	}

	async fn check_out_artifact_with_files(
		&self,
		id: &tg::artifact::Id,
		arg: &tg::artifact::checkout::Arg,
		files: Arc<DashMap<tg::file::Id, tg::Path, fnv::FnvBuildHasher>>,
		sender: &async_channel::Sender<tg::Result<tg::artifact::checkout::Event>>,
		count: &AtomicU64,
		weight: &AtomicU64,
	) -> tg::Result<tg::Path> {
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
				return Err(tg::error!(%path, "the path must be absolute"));
			}
			let exists = tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to stat the path"))?;
			if exists && !arg.force {
				return Err(tg::error!(%path, "there is already a file system object at the path"));
			}
			if (path.as_ref() as &std::path::Path).starts_with(self.checkouts_path()) {
				return Err(tg::error!(%path, "cannot check out into the checkouts directory"));
			}

			// Check in an existing artifact at the path.
			let existing_artifact = if exists {
				let artifact = tg::Artifact::check_in(self, path.clone()).await?;
				Some(artifact)
			} else {
				None
			};

			// Perform the checkout.
			self.check_out_inner(
				&path,
				&artifact,
				existing_artifact.as_ref(),
				arg,
				0,
				files,
				sender,
				count,
				weight,
			)
			.await?;

			Ok(path)
		} else {
			// Get the path in the checkouts directory.
			let id = artifact.id(self).await?;
			let path: tg::Path = self.checkouts_path().join(id.to_string()).try_into()?;
			let artifact_path = self.artifacts_path().join(id.to_string()).try_into()?;

			// If there is already a file system object at the path, then return.
			if tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to stat the path"))?
			{
				return Ok(artifact_path);
			}

			// Create a tmp.
			let tmp = Tmp::new(self);

			// Perform the checkout to the tmp.
			let existing = Arc::new(DashMap::default());
			self.check_out_inner(
				&tmp.path.clone().try_into()?,
				&artifact,
				None,
				arg,
				0,
				existing,
				sender,
				count,
				weight,
			)
			.await?;

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
						tg::error!(!source, %tmp = tmp.path.display(), %path, "failed to move the checkout to the checkouts directory"),
					);
				},
			};

			Ok(artifact_path)
		}
	}

	async fn check_out_inner(
		&self,
		path: &tg::Path,
		artifact: &tg::Artifact,
		existing_artifact: Option<&tg::Artifact>,
		arg: &tg::artifact::checkout::Arg,
		depth: usize,
		files: Arc<DashMap<tg::file::Id, tg::Path, fnv::FnvBuildHasher>>,
		sender: &async_channel::Sender<tg::Result<tg::artifact::checkout::Event>>,
		count: &AtomicU64,
		weight: &AtomicU64,
	) -> tg::Result<()> {
		// If the artifact is the same as the existing artifact, then return.
		let id = artifact.id(self).await?;
		match existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if id == existing_artifact.id(self).await? {
					let metadata = self.get_object_metadata(&id.clone().into()).await?;
					let count = metadata
						.count
						.map(|count_| count.fetch_add(count_, Ordering::Relaxed));
					let weight = metadata
						.weight
						.map(|weight_| weight.fetch_add(weight_, Ordering::Relaxed));
					if let (Some(count), Some(weight)) = (count, weight) {
						let event = tg::artifact::checkout::Event::Progress(
							tg::artifact::checkout::Progress {
								count: tg::Progress {
									current: count,
									total: None,
								},
								weight: tg::Progress {
									current: weight,
									total: None,
								},
							},
						);
						sender.try_send(Ok(event)).ok();
					}
					return Ok(());
				}
			},
		}

		// Call the appropriate function for the artifact's type.
		match artifact {
			tg::Artifact::Directory(directory) => {
				Box::pin(self.check_out_directory(
					path,
					directory,
					existing_artifact,
					arg,
					depth,
					files,
					sender,
					count,
					weight,
				))
				.await
				.map_err(
					|source| tg::error!(!source, %id, %path, "failed to check out the directory"),
				)?;
			},

			tg::Artifact::File(file) => {
				Box::pin(self.check_out_file(
					path,
					file,
					existing_artifact,
					arg,
					files,
					sender,
					count,
					weight,
				))
				.await
				.map_err(
					|source| tg::error!(!source, %id, %path, "failed to check out the file"),
				)?;
			},

			tg::Artifact::Symlink(symlink) => {
				Box::pin(self.check_out_symlink(
					path,
					symlink,
					existing_artifact,
					arg,
					depth,
					files,
					sender,
					count,
					weight,
				))
				.await
				.map_err(
					|source| tg::error!(!source, %id, %path, "failed to check out the symlink"),
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

	async fn check_out_directory(
		&self,
		path: &tg::Path,
		directory: &tg::Directory,
		existing_artifact: Option<&tg::Artifact>,
		arg: &tg::artifact::checkout::Arg,
		depth: usize,
		files: Arc<DashMap<tg::file::Id, tg::Path, fnv::FnvBuildHasher>>,
		sender: &async_channel::Sender<tg::Result<tg::artifact::checkout::Event>>,
		count: &AtomicU64,
		weight: &AtomicU64,
	) -> tg::Result<()> {
		// Handle an existing artifact at the path.
		match existing_artifact {
			// If there is an existing directory, then remove any extraneous entries.
			Some(tg::Artifact::Directory(existing_directory)) => {
				existing_directory
					.entries(self)
					.await?
					.iter()
					.map(|(name, _)| async move {
						if !directory.entries(self).await?.contains_key(name) {
							let entry_path = path.clone().join(name);
							remove(&entry_path).await.ok();
						}
						Ok::<_, tg::Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
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
				let existing = files.clone();
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
					self.check_out_inner(
						&entry_path,
						artifact,
						existing_artifact.as_ref(),
						arg,
						depth + 1,
						existing,
						sender,
						count,
						weight,
					)
					.await?;

					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		Ok(())
	}

	async fn check_out_file(
		&self,
		path: &tg::Path,
		file: &tg::File,
		existing_artifact: Option<&tg::Artifact>,
		arg: &tg::artifact::checkout::Arg,
		files: Arc<DashMap<tg::file::Id, tg::Path, fnv::FnvBuildHasher>>,
		sender: &async_channel::Sender<tg::Result<tg::artifact::checkout::Event>>,
		count: &AtomicU64,
		weight: &AtomicU64,
	) -> tg::Result<()> {
		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				remove(path).await.ok();
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Check out the file's references.
		let references = file
			.references(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's references"))?
			.iter()
			.map(|artifact| artifact.id(self))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<BTreeSet<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's references"))?;
		if arg.references && !references.is_empty() {
			references
				.iter()
				.map(|artifact| async {
					let arg = tg::artifact::checkout::Arg {
						bundle: false,
						force: false,
						path: None,
						references: true,
					};
					Box::pin(self.check_out_artifact_with_files(
						artifact,
						&arg,
						files.clone(),
						sender,
						count,
						weight,
					))
					.await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await
				.map_err(|error| {
					tg::error!(source = error, "failed to check out the file's references")
				})?;
		}

		// Attempt to copy the file from another file in the checkout.
		let id = file.id(self).await?;
		let existing_path = files.get(&id).map(|path| path.clone());
		if let Some(existing_path) = existing_path {
			let permit = self.file_descriptor_semaphore.acquire().await;
			tokio::fs::copy(&existing_path, &path).await.map_err(
				|source| tg::error!(!source, %existing_path, %to = &path, %id, "failed to copy the file"),
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

		// Set the extended attributes if necessary.
		if !references.is_empty() {
			let attributes = tg::file::Attributes { references };
			let attributes = serde_json::to_vec(&attributes)
				.map_err(|source| tg::error!(!source, "failed to serialize attributes"))?;
			xattr::set(path, tg::file::TANGRAM_FILE_XATTR_NAME, &attributes)
				.map_err(|source| tg::error!(!source, "failed to set attributes as an xattr"))?;
		}

		// Add the path the files map.
		files.insert(id.clone(), path.clone());

		Ok(())
	}

	async fn check_out_symlink(
		&self,
		path: &tg::Path,
		symlink: &tg::Symlink,
		existing_artifact: Option<&tg::Artifact>,
		arg: &tg::artifact::checkout::Arg,
		depth: usize,
		files: Arc<DashMap<tg::file::Id, tg::Path, fnv::FnvBuildHasher>>,
		sender: &async_channel::Sender<tg::Result<tg::artifact::checkout::Event>>,
		count: &AtomicU64,
		weight: &AtomicU64,
	) -> tg::Result<()> {
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
		if arg.references {
			if let Some(artifact) = symlink.artifact(self).await? {
				if arg.path.is_some() {
					return Err(tg::error!(
						r#"cannot perform an external check out of a symlink with an artifact"#
					));
				}
				let id = artifact.id(self).await?;
				let arg = tg::artifact::checkout::Arg::default();
				Box::pin(
					self.check_out_artifact_with_files(&id, &arg, files, sender, count, weight),
				)
				.await?;
			}
		}

		// Render the target.
		let mut target = tg::Path::new();
		let artifact = symlink.artifact(self).await?;
		let path_ = symlink.path(self).await?;
		if let Some(artifact) = artifact.as_ref() {
			for _ in 0..depth {
				target.push(tg::path::Component::Parent);
			}
			target = target.join("../../.tangram/artifacts".parse::<tg::Path>().unwrap());
			let id = &artifact.id(self).await?;
			let component = tg::path::Component::Normal(id.to_string());
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
		let sse = stream.map(|result| match result {
			Ok(tg::artifact::checkout::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::artifact::checkout::Event::End(path)) => {
				let event = "end".to_owned();
				let data = serde_json::to_string(&path).unwrap();
				let event = tangram_http::sse::Event {
					event: Some(event),
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Err(error) => {
				let data = serde_json::to_string(&error).unwrap();
				let event = "error".to_owned();
				let event = tangram_http::sse::Event {
					data,
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}
