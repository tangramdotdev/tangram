use crate::{database, Http, Server};
use async_recursion::async_recursion;
use futures::{stream::FuturesUnordered, TryStreamExt};
use http_body_util::BodyExt;
use std::os::unix::prelude::PermissionsExt;
use tangram_client as tg;
use tangram_error::{error, Error, Result, Wrap, WrapErr};
use tangram_util::{
	fs::rmrf,
	http::{full, ok, Incoming, Outgoing},
};
use tg::Handle;

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput> {
		let path = arg.path;

		let server = self.clone();
		let task = self
			.inner
			.local_pool_handle
			.spawn_pinned(move || async move {
				// Get the database and create a transaction.
				let mut connection = server.inner.database.get().await?;
				let txn = connection.transaction().await?;

				// Check in the artifact and commit the transaction.
				let id = server.check_in_artifact_inner(&path, &txn).await?;
				txn.commit().await?;

				Ok::<_, Error>(id)
			});

		let abort = task.abort_handle();
		scopeguard::defer! {
			abort.abort();
		}
		let id = task.await.wrap_err("Failed to join check in task.")??;
		let output = tg::artifact::CheckInOutput { id };

		Ok(output)
	}

	async fn check_in_artifact_inner(
		&self,
		path: &tg::Path,
		txn: &database::Transaction<'_>,
	) -> Result<tg::artifact::Id> {
		// Get the metadata for the file system object at the path.
		let metadata = tokio::fs::symlink_metadata(&path)
			.await
			.wrap_err_with(|| format!(r#"Failed to get the metadata for the path "{path}"."#))?;

		// Call the appropriate function for the file system object at the path.
		if metadata.is_dir() {
			self.check_in_directory(path, &metadata, txn)
				.await
				.wrap_err_with(|| format!(r#"Failed to check in the directory at path "{path}"."#))
		} else if metadata.is_file() {
			self.check_in_file(path, &metadata, txn)
				.await
				.wrap_err_with(|| format!(r#"Failed to check in the file at path "{path}"."#))
		} else if metadata.is_symlink() {
			self.check_in_symlink(path, &metadata, txn)
				.await
				.wrap_err_with(|| format!(r#"Failed to check in the symlink at path "{path}"."#))
		} else {
			Err(error!(
				"The path must point to a directory, file, or symlink."
			))
		}
	}

	#[async_recursion(?Send)]
	async fn check_in_directory<'a>(
		&'a self,
		path: &'a tg::Path,
		_metadata: &'a std::fs::Metadata,
		txn: &'a database::Transaction<'_>,
	) -> Result<tg::artifact::Id> {
		let names = {
			let _permit = self.file_descriptor_semaphore().acquire().await;
			let mut read_dir = tokio::fs::read_dir(path)
				.await
				.wrap_err("Failed to read the directory.")?;
			let mut names = Vec::new();
			while let Some(entry) = read_dir
				.next_entry()
				.await
				.wrap_err("Failed to get the directory entry.")?
			{
				let name = entry
					.file_name()
					.to_str()
					.wrap_err("All file names must be valid UTF-8.")?
					.to_owned();
				names.push(name);
			}
			names
		};

		// Recurse into the directory's entries.
		let entries = names
			.into_iter()
			.map(|name| async {
				let path = path.clone().join(&name);
				let id = self.check_in_artifact_inner(&path, txn).await?;
				Ok::<_, Error>((name, id))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Create the directory.
		let data = tg::directory::Data { entries };
		let id = tg::directory::Id::new(&data.serialize()?);
		self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
			.await?;
		Ok(id.into())
	}

	async fn check_in_file(
		&self,
		path: &tg::Path,
		metadata: &std::fs::Metadata,
		txn: &database::Transaction<'_>,
	) -> Result<tg::artifact::Id> {
		// Create the blob.
		let permit = self.file_descriptor_semaphore().acquire().await;
		let file = tokio::fs::File::open(path)
			.await
			.wrap_err("Failed to open the file.")?;
		let contents = self
			.create_blob_with_reader(file, txn)
			.await
			.wrap_err("Failed to create the contents.")?;
		drop(permit);

		// Determine if the file is executable.
		let executable = (metadata.permissions().mode() & 0o111) != 0;

		// Read the file's references from its xattrs.
		let attributes: Option<tg::file::Attributes> =
			xattr::get(path, tg::file::TANGRAM_FILE_XATTR_NAME)
				.ok()
				.flatten()
				.and_then(|attributes| serde_json::from_slice(&attributes).ok());
		let references = attributes
			.map(|attributes| attributes.references)
			.unwrap_or_default()
			.into_iter()
			.collect();

		// Create the file.
		let data = tg::file::Data {
			contents,
			executable,
			references,
		};

		// Get the data and ID without putting the object.
		let id = tg::file::Id::new(&data.serialize()?);
		self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
			.await?;

		Ok(id.into())
	}

	async fn check_in_symlink(
		&self,
		path: &tg::Path,
		_metadata: &std::fs::Metadata,
		txn: &database::Transaction<'_>,
	) -> Result<tg::artifact::Id> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(path)
			.await
			.wrap_err_with(|| format!(r#"Failed to read the symlink at path "{path}"."#,))?;

		// Unrender the target.
		let target = target
			.to_str()
			.wrap_err("The symlink target must be valid UTF-8.")?;
		let target = tg::Template::unrender(target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.wrap_err("Invalid symlink.")?
				.clone();
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.wrap_err("Invalid symlink.")?;
			let artifact = match artifact {
				tg::Artifact::Directory(directory) => {
					directory.state().read().unwrap().id.clone().unwrap().into()
				},
				tg::Artifact::File(file) => file.state().read().unwrap().id.clone().unwrap().into(),
				tg::Artifact::Symlink(symlink) => {
					symlink.state().read().unwrap().id.clone().unwrap().into()
				},
			};
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.wrap_err("Invalid sylink.")?
				.clone();
			(Some(artifact), Some(path))
		} else {
			return Err(error!("Invalid symlink."));
		};

		// Create the symlink.
		let data = tg::symlink::Data { artifact, path };
		let id = tg::symlink::Id::new(&data.serialize()?);
		self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
			.await?;

		Ok(id.into())
	}

	pub async fn check_out_artifact(&self, arg: tg::artifact::CheckOutArg) -> Result<()> {
		let artifact = tg::Artifact::with_id(arg.artifact);
		if let Some(path) = &arg.path {
			// Bundle the artifact.
			let artifact = artifact
				.bundle(self)
				.await
				.wrap_err("Failed to bundle the artifact.")?;

			// Check in an existing artifact at the path.
			let existing_artifact = if tokio::fs::try_exists(path)
				.await
				.wrap_err("Failed to determine if the path exists.")?
			{
				let arg = tg::artifact::CheckInArg { path: path.clone() };
				let output = self.check_in_artifact(arg).await?;
				Some(tg::Artifact::with_id(output.id))
			} else {
				None
			};

			// Perform the checkout.
			self.check_out_inner(path, &artifact, existing_artifact.as_ref(), false, 0)
				.await?;

			Ok(())
		} else {
			// Get the path in the checkouts directory.
			let id = artifact.id(self).await?;
			let path = self.checkouts_path().join(id.to_string());

			// If there is already a file system object at the path, then return.
			if tokio::fs::try_exists(&path)
				.await
				.wrap_err("Failed to stat the path.")?
			{
				return Ok(());
			}

			// Create a tmp path.
			let tmp = self.create_tmp();

			// Perform the checkout to the tmp path.
			self.check_out_inner(&tmp.path.clone().try_into()?, &artifact, None, true, 0)
				.await?;

			// Move the checkout to the checkouts directory.
			match tokio::fs::rename(&tmp, &path).await {
				Ok(()) => (),
				// If the entry in the checkouts directory exists, then remove the checkout at the tmp path.
				Err(ref error)
					if matches!(error.raw_os_error(), Some(libc::ENOTEMPTY | libc::EEXIST)) =>
				{
					rmrf(&tmp).await?;
				},
				Err(error) => {
					return Err(
						error.wrap("Failed to move the checkout to the checkouts directory.")
					);
				},
			};

			Ok(())
		}
	}

	#[async_recursion]
	async fn check_out_inner(
		&self,
		path: &tg::Path,
		artifact: &tg::Artifact,
		existing_artifact: Option<&'async_recursion tg::Artifact>,
		internal: bool,
		depth: usize,
	) -> Result<()> {
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
		match artifact {
			tg::Artifact::Directory(directory) => {
				self.check_out_directory(path, directory, existing_artifact, internal, depth)
					.await
					.wrap_err_with(|| {
						format!(r#"Failed to check out directory "{id}" to "{path}"."#)
					})?;
			},

			tg::Artifact::File(file) => {
				self.check_out_file(path, file, existing_artifact, internal, depth)
					.await
					.wrap_err_with(|| format!(r#"Failed to check out file "{id}" to "{path}"."#))?;
			},

			tg::Artifact::Symlink(symlink) => {
				self.check_out_symlink(path, symlink, existing_artifact, internal, depth)
					.await
					.wrap_err_with(|| {
						format!(r#"Failed to check out symlink "{id}" to "{path}"."#)
					})?;
			},
		}

		// If this is an internal checkout, then set the file system object's modified time to the epoch.
		tokio::task::spawn_blocking({
			let path = path.clone();
			move || {
				let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
				filetime::set_symlink_file_times(path, epoch, epoch)
					.wrap_err("Failed to set the modified time.")?;
				Ok::<_, Error>(())
			}
		})
		.await
		.unwrap()?;

		Ok(())
	}

	async fn check_out_directory(
		&self,
		path: &tg::Path,
		directory: &tg::Directory,
		existing_artifact: Option<&tg::Artifact>,
		internal: bool,
		depth: usize,
	) -> Result<()> {
		// Handle an existing artifact at the path.
		match existing_artifact {
			// If there is already a directory, then remove any extraneous entries.
			Some(tg::Artifact::Directory(existing_directory)) => {
				existing_directory
					.entries(self)
					.await?
					.iter()
					.map(|(name, _)| async move {
						if !directory.entries(self).await?.contains_key(name) {
							let entry_path = path.clone().join(name);
							rmrf(&entry_path).await?;
						}
						Ok::<_, Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
			},

			// If there is an existing artifact at the path and it is not a directory, then remove it, create a directory, and continue.
			Some(_) => {
				rmrf(path).await?;
				tokio::fs::create_dir_all(path)
					.await
					.wrap_err("Failed to create the directory.")?;
			},

			// If there is no artifact at this path, then create a directory.
			None => {
				tokio::fs::create_dir_all(path)
					.await
					.wrap_err("Failed to create the directory.")?;
			},
		}

		// Recurse into the entries.
		directory
			.entries(self)
			.await?
			.iter()
			.map(|(name, artifact)| {
				let existing_artifact = &existing_artifact;
				async move {
					// Retrieve an existing artifact.
					let existing_artifact = match existing_artifact {
						Some(tg::Artifact::Directory(existing_directory)) => {
							let name = name.parse().wrap_err("Invalid entry name.")?;
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
						internal,
						depth + 1,
					)
					.await?;

					Ok::<_, Error>(())
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
		internal: bool,
		depth: usize,
	) -> Result<()> {
		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				rmrf(path).await?;
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Check out the file's references.
		let references = file
			.references(self)
			.await
			.wrap_err("Failed to get the file's references.")?
			.iter()
			.map(|artifact| artifact.id(self))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await
			.wrap_err("Failed to get the file's references.")?;
		if !references.is_empty() {
			if !internal {
				return Err(error!(
					r#"Cannot perform an external check out of a file with references."#
				));
			}
			references
				.iter()
				.map(|artifact| async {
					let arg = tg::artifact::CheckOutArg {
						artifact: artifact.clone(),
						path: None,
					};
					self.check_out_artifact(arg).await?;
					Ok::<_, Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await
				.wrap_err("Failed to check out the file's references.")?;
		}

		if internal && depth > 0 {
			let arg = tg::artifact::CheckOutArg {
				artifact: file.id(self).await?.clone().into(),
				path: None,
			};
			self.check_out_artifact(arg).await?;
			let src = self.artifacts_path().join(file.id(self).await?.to_string());
			tokio::fs::hard_link(&src, path)
				.await
				.wrap_err("Failed to create the hard link.")?;
			return Ok(());
		}

		// Create the file.
		let permit = self.file_descriptor_semaphore().acquire().await;
		tokio::io::copy(
			&mut file.reader(self).await?,
			&mut tokio::fs::File::create(path)
				.await
				.wrap_err("Failed to create the file.")?,
		)
		.await
		.wrap_err("Failed to write the bytes.")?;
		drop(permit);

		// Make the file executable if necessary.
		if file.executable(self).await? {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(path, permissions)
				.await
				.wrap_err("Failed to set the permissions.")?;
		}

		// Set the extended attributes if necessary.
		if internal && !references.is_empty() {
			let attributes = tg::file::Attributes { references };
			let attributes =
				serde_json::to_vec(&attributes).wrap_err("Failed to serialize attributes.")?;
			xattr::set(path, tg::file::TANGRAM_FILE_XATTR_NAME, &attributes)
				.wrap_err("Failed to set attributes as an xattr.")?;
		}

		Ok(())
	}

	async fn check_out_symlink(
		&self,
		path: &tg::Path,
		symlink: &tg::Symlink,
		existing_artifact: Option<&tg::Artifact>,
		internal: bool,
		depth: usize,
	) -> Result<()> {
		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				rmrf(&path).await?;
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Check out the symlink's artifact if necessary.
		if let Some(artifact) = symlink.artifact(self).await? {
			if !internal {
				return Err(error!(
					r#"Cannot perform an external check out of a symlink with an artifact."#
				));
			}
			let arg = tg::artifact::CheckOutArg {
				artifact: artifact.id(self).await?.clone(),
				path: None,
			};
			self.check_out_artifact(arg).await?;
		}

		// Render the target.
		let mut target = String::new();
		let artifact = symlink.artifact(self).await?;
		let path_ = symlink.path(self).await?;
		if let Some(artifact) = artifact {
			for _ in 0..depth {
				target.push_str("../");
			}
			target.push_str("../../.tangram/artifacts/");
			target.push_str(&artifact.id(self).await?.to_string());
		}
		if artifact.is_some() && path_.is_some() {
			target.push('/');
		}
		if let Some(path) = path_ {
			target.push_str(path);
		}

		// Create the symlink.
		tokio::fs::symlink(target, path)
			.await
			.wrap_err("Failed to create the symlink")?;

		Ok(())
	}
}

impl Http {
	pub async fn handle_check_in_artifact_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		let output = self.inner.tg.check_in_artifact(arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	pub async fn handle_check_out_artifact_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Check out the artifact.
		self.inner.tg.check_out_artifact(arg).await?;

		Ok(ok())
	}
}
