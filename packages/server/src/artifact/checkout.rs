use crate::Server;
use dashmap::{DashMap, DashSet};
use futures::{
	stream::FuturesUnordered, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::{os::unix::fs::PermissionsExt as _, panic::AssertUnwindSafe, path::PathBuf, sync::Arc};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tokio_util::{io::InspectReader, task::AbortOnDropHandle};

mod lockfile;

#[derive(Clone, Debug)]
struct State {
	artifacts_path: Option<PathBuf>,
	files: DashMap<tg::file::Id, PathBuf, fnv::FnvBuildHasher>,
	progress: crate::progress::Handle<tg::artifact::checkout::Output>,
	visited_dependencies: DashSet<tg::artifact::Id, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug)]
struct Arg {
	artifact: tg::artifact::Id,
	dependencies: bool,
	existing_artifact: Option<tg::Artifact>,
	path: PathBuf,
	root_artifact: tg::artifact::Id,
	root_path: Arc<PathBuf>,
}

#[derive(Clone, Debug)]
struct Output {
	progress: Progress,
}

#[derive(Clone, Debug, Default)]
struct Progress {
	objects: u64,
	bytes: u64,
}

impl Server {
	pub async fn check_out_artifact(
		&self,
		artifact: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>
			+ Send
			+ 'static,
	> {
		let metadata = self
			.try_get_object_metadata(&artifact.clone().into())
			.await?;
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let artifact = artifact.clone();
			let arg = arg.clone();
			let progress = progress.clone();
			async move {
				let count = metadata.as_ref().and_then(|metadata| metadata.count);
				let weight = metadata.as_ref().and_then(|metadata| metadata.weight);
				progress.start(
					"objects".to_owned(),
					"objects".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					count,
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					weight,
				);
				let result =
					AssertUnwindSafe(server.check_out_artifact_task(artifact, arg, &progress))
						.catch_unwind()
						.await;
				progress.finish("objects");
				progress.finish("bytes");
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				};
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	async fn check_out_artifact_task(
		&self,
		artifact: tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
		match arg.path.clone() {
			None => {
				if !self.vfs.lock().unwrap().is_some() {
					self.cache_artifact(artifact.clone(), progress)
						.await
						.map_err(|source| tg::error!(!source, "failed to cache the artifact"))?;
				}
				let path = self.artifacts_path().join(artifact.to_string());
				let output = tg::artifact::checkout::Output { path };
				Ok(output)
			},
			Some(path) => {
				self.check_out_artifact_task_inner(artifact, arg, path, progress)
					.await
			},
		}
	}

	async fn check_out_artifact_task_inner(
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
				cache: false,
				destructive: false,
				deterministic: true,
				ignore: true,
				locked: true,
				lockfile: false,
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
		let arg_ = Arg {
			artifact: artifact.clone(),
			dependencies: arg.dependencies,
			existing_artifact,
			path: path.clone(),
			root_artifact: artifact.clone(),
			root_path: Arc::new(path.clone()),
		};

		// Perform the checkout.
		self.check_out_artifact_inner(&state, arg_).await?;

		// Create a lockfile and write it if it is not empty.
		'a: {
			// Skip creation if this is a symlink or the user passed lockfile: false
			if artifact.is_symlink() || !arg.lockfile {
				break 'a;
			}

			// Create the lockfile.
			let artifact = tg::Artifact::with_id(artifact.clone());
			let lockfile = self
				.create_lockfile_for_artifact(&artifact, arg.dependencies)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the lockfile"))?;

			// Skip creation if the lockfile is empty.
			if lockfile.nodes.is_empty() {
				break 'a;
			}

			// If this is a directory, write it as a child.
			if artifact.is_directory() {
				// Serialize the lockfile.
				let contents = serde_json::to_vec_pretty(&lockfile)
					.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;

				let lockfile_path = path.join(tg::package::LOCKFILE_FILE_NAME);
				let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
				tokio::fs::write(&lockfile_path, &contents).await.map_err(
					|source| tg::error!(!source, %path = lockfile_path.display(), "failed to write the lockfile"),
				)?;
			} else {
				// Serialize the lockfile.
				let contents = serde_json::to_vec(&lockfile)
					.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;

				let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
				xattr::set(&path, tg::file::XATTR_LOCK_NAME, &contents).map_err(|source| {
					tg::error!(!source, "failed to write the lockfile contents as an xattr")
				})?;
			}
		}

		// Create the output.
		let output = tg::artifact::checkout::Output { path };

		Ok(output)
	}

	async fn check_out_artifact_dependency(
		&self,
		state: &State,
		artifact: tg::artifact::Id,
	) -> tg::Result<Output> {
		// Mark the dependency as visited and exit early if it has already been visited.
		if !state.visited_dependencies.insert(artifact.clone()) {
			let output = Output {
				progress: Progress::default(),
			};
			return Ok(output);
		}

		// Get the artifacts path.
		let Some(artifacts_path) = state.artifacts_path.as_ref() else {
			return Err(tg::error!(
				"cannot check out a dependency without an artifacts path"
			));
		};

		// Create the artifacts directory.
		tokio::fs::create_dir_all(&artifacts_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the artifacts directory"))?;

		// Create the arg.
		let path = artifacts_path.join(artifact.to_string());
		let arg = Arg {
			artifact: artifact.clone(),
			dependencies: true,
			existing_artifact: None,
			path: path.clone(),
			root_artifact: artifact.clone(),
			root_path: Arc::new(path.clone()),
		};

		// Perform the checkout.
		let output = self.check_out_artifact_inner(state, arg).await?;

		Ok(output)
	}

	async fn check_out_artifact_inner(&self, state: &State, arg: Arg) -> tg::Result<Output> {
		let artifact_id = arg.artifact.clone().into();

		// If the artifact is the same as the existing artifact, then return.
		match &arg.existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if arg.artifact == existing_artifact.id(self).await? {
					let output = Output {
						progress: Progress::default(),
					};
					return Ok(output);
				}
			},
		}

		// Check out the artifact.
		let mut output = match arg.artifact.clone() {
			tg::artifact::Id::Directory(directory) => {
				self.check_out_directory(state, arg, &directory).await?
			},
			tg::artifact::Id::File(file) => self.check_out_file(state, arg, &file).await?,
			tg::artifact::Id::Symlink(symlink) => {
				self.check_out_symlink(state, arg, &symlink).await?
			},
		};

		// Increment the progress.
		let metadata = self.get_object_metadata(&artifact_id).await;
		if let Ok(metadata) = metadata {
			if let Some(count) = metadata.count {
				let objects = count.saturating_sub(output.progress.objects);
				state.progress.increment("objects", objects);
				output.progress.objects += objects;
			}
			if let Some(weight) = metadata.weight {
				let bytes = weight.saturating_sub(output.progress.bytes);
				state.progress.increment("bytes", bytes);
				output.progress.bytes += bytes;
			}
		}

		Ok(output)
	}

	async fn check_out_directory(
		&self,
		state: &State,
		arg: Arg,
		directory: &tg::directory::Id,
	) -> tg::Result<Output> {
		let mut output = Output {
			progress: Progress::default(),
		};
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
		let outputs = directory
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
						dependencies: arg.dependencies,
						existing_artifact,
						path,
						root_artifact: arg.root_artifact,
						root_path: arg.root_path,
					};
					let progress = server.check_out_artifact_inner(&state, arg).await?;
					Ok::<_, tg::Error>(progress)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<Output>>()
			.await?;
		output.progress += outputs.into_iter().map(|output| output.progress).sum();

		Ok(output)
	}

	async fn check_out_file(
		&self,
		state: &State,
		arg: Arg,
		file: &tg::file::Id,
	) -> tg::Result<Output> {
		let mut output = Output {
			progress: Progress::default(),
		};
		let id = file.clone();
		let file = tg::File::with_id(id.clone());

		// Handle an existing artifact at the path.
		if arg.existing_artifact.is_some() {
			crate::util::fs::remove(&arg.path).await.ok();
		};

		// Check out the file's dependencies.
		if arg.dependencies {
			let dependency_outputs = file
				.dependencies(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?
				.into_values()
				.filter_map(|referent| tg::Artifact::try_from(referent.item).ok())
				.map(|artifact| {
					let server = self.clone();
					let state = state.clone();
					async move {
						let artifact = artifact.id(&server).await?;
						let progress = server
							.check_out_artifact_dependency(&state, artifact)
							.await?;
						Ok::<_, tg::Error>(progress)
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<Output>>()
				.await?;
			output.progress += dependency_outputs
				.into_iter()
				.map(|output| output.progress)
				.sum();
		};

		// Attempt to copy the file from another file in the checkout.
		let path = state.files.get(&id).map(|path| path.clone());
		if let Some(path) = path {
			let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
			tokio::fs::copy(&path, &arg.path).await.map_err(
				|source| tg::error!(!source, %src = path.display(), %dst = &arg.path.display(), %file, "failed to copy the file"),
			)?;
			drop(permit);
			return Ok(output);
		}

		// Attempt to copy the file from the cache directory.
		let cache_path = self.cache_path().join(id.to_string());
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let result = tokio::fs::copy(&cache_path, &arg.path).await;
		drop(permit);
		if result.is_ok() {
			return Ok(output);
		}

		// Otherwise, create the file.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let reader = file
			.read(self, tg::blob::read::Arg::default())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the reader"))?;
		let mut reader = InspectReader::new(reader, |slice| {
			output.progress.bytes += slice.len().to_u64().unwrap();
			state.progress.increment("bytes", slice.len() as u64);
		});
		let mut file_ = tokio::fs::File::create(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the file"))?;
		tokio::io::copy(&mut reader, &mut file_).await.map_err(
			|source| tg::error!(!source, ?path = arg.path, "failed to write to the file"),
		)?;
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

		// If the artifact is a file, then add its path to the files map.
		if let Ok(file) = arg.artifact.try_unwrap_file_ref() {
			if let dashmap::Entry::Vacant(entry) = state.files.entry(file.clone()) {
				entry.insert(arg.path.clone());
			}
		}

		Ok(output)
	}

	async fn check_out_symlink(
		&self,
		state: &State,
		arg: Arg,
		symlink: &tg::symlink::Id,
	) -> tg::Result<Output> {
		let mut output = Output {
			progress: Progress::default(),
		};
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
		if arg.dependencies {
			if let Some(artifact) = &artifact {
				if artifact.id(self).await? != arg.root_artifact {
					let server = self.clone();
					let artifact = artifact.id(self).await?;
					let dependency_output =
						Box::pin(server.check_out_artifact_dependency(state, artifact)).await?;
					output.progress += dependency_output.progress;
				}
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

		Ok(output)
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
