use crate::Server;
use async_recursion::async_recursion;
use futures::{stream::FuturesUnordered, TryStreamExt};
use std::os::unix::prelude::PermissionsExt;
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tg::Handle;

impl Server {
	#[async_recursion]
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput> {
		let path = &arg.path;

		// Get the metadata for the file system object at the path.
		let metadata = tokio::fs::symlink_metadata(path)
			.await
			.wrap_err_with(|| format!(r#"Failed to get the metadata for the path "{path}"."#))?;

		// Call the appropriate function for the file system object at the path.
		let id = if metadata.is_dir() {
			self.check_in_directory(path, &metadata)
				.await
				.wrap_err_with(|| {
					format!(r#"Failed to check in the directory at path "{path}"."#)
				})?
		} else if metadata.is_file() {
			self.check_in_file(path, &metadata)
				.await
				.wrap_err_with(|| format!(r#"Failed to check in the file at path "{path}"."#))?
		} else if metadata.is_symlink() {
			self.check_in_symlink(path, &metadata)
				.await
				.wrap_err_with(|| format!(r#"Failed to check in the symlink at path "{path}"."#))?
		} else {
			return Err(error!(
				"The path must point to a directory, file, or symlink."
			));
		};

		let output = tg::artifact::CheckInOutput { id };

		Ok(output)
	}

	async fn check_in_directory(
		&self,
		path: &tg::Path,
		_metadata: &std::fs::Metadata,
	) -> Result<tg::artifact::Id> {
		// Read the contents of the directory.
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
				let path = path.clone().join(name.clone().try_into()?);
				let arg = tg::artifact::CheckInArg { path };
				let output = self.check_in_artifact(arg).await?;
				let artifact = tg::Artifact::with_id(output.id);
				Ok::<_, Error>((name, artifact))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Create the directory.
		let directory = tg::Directory::new(entries);
		let id = directory.id(self).await?.clone();

		Ok(id.into())
	}

	async fn check_in_file(
		&self,
		path: &tg::Path,
		metadata: &std::fs::Metadata,
	) -> Result<tg::artifact::Id> {
		// Create the blob.
		let permit = self.file_descriptor_semaphore().acquire().await;
		let file = tokio::fs::File::open(path)
			.await
			.wrap_err("Failed to open the file.")?;
		let contents = tg::Blob::with_reader(self, file)
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
			.map(tg::Artifact::with_id)
			.collect();

		// Create the file.
		let file = tg::File::new(contents, executable, references);
		let id = file.id(self).await?.clone();

		Ok(id.into())
	}

	async fn check_in_symlink(
		&self,
		path: &tg::Path,
		_metadata: &std::fs::Metadata,
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
				.wrap_err("Invalid sylink.")?
				.clone();
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.wrap_err("Invalid sylink.")?
				.clone();
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
		let symlink = tg::Symlink::new(artifact, path);
		let id = symlink.id(self).await?.clone();

		Ok(id.into())
	}

	pub async fn check_out_artifact(&self, arg: tg::artifact::CheckOutArg) -> Result<()> {
		let artifact = tg::Artifact::with_id(arg.artifact);

		// Bundle the artifact.
		let artifact = artifact
			.bundle(self)
			.await
			.wrap_err("Failed to bundle the artifact.")?;

		// Check in an existing artifact at the path.
		let existing_artifact = if tokio::fs::try_exists(&arg.path)
			.await
			.wrap_err("Failed to determine if the path exists.")?
		{
			let arg = tg::artifact::CheckInArg {
				path: arg.path.clone(),
			};
			let output = self.check_in_artifact(arg).await?;
			Some(tg::Artifact::with_id(output.id))
		} else {
			None
		};

		// Check out the artifact recursively.
		self.check_out_inner(&artifact, existing_artifact.as_ref(), &arg.path)
			.await?;

		Ok(())
	}

	async fn check_out_inner(
		&self,
		artifact: &tg::Artifact,
		existing_artifact: Option<&tg::Artifact>,
		path: &tg::Path,
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
				self.check_out_directory(existing_artifact, directory, path)
					.await
					.wrap_err_with(|| {
						format!(r#"Failed to check out directory "{id}" to "{path}"."#)
					})?;
			},

			tg::Artifact::File(file) => {
				self.check_out_file(existing_artifact, file, path)
					.await
					.wrap_err_with(|| format!(r#"Failed to check out file "{id}" to "{path}"."#))?;
			},

			tg::Artifact::Symlink(symlink) => {
				self.check_out_symlink(existing_artifact, symlink, path)
					.await
					.wrap_err_with(|| {
						format!(r#"Failed to check out symlink "{id}" to "{path}"."#)
					})?;
			},
		}

		Ok(())
	}

	#[async_recursion]
	async fn check_out_directory(
		&self,
		existing_artifact: Option<&'async_recursion tg::Artifact>,
		directory: &tg::Directory,
		path: &tg::Path,
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
							let entry_path = path.clone().join(name.parse()?);
							tg::util::rmrf(&entry_path).await?;
						}
						Ok::<_, Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
			},

			// If there is an existing artifact at the path and it is not a directory, then remove it, create a directory, and continue.
			Some(_) => {
				tg::util::rmrf(path).await?;
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
					let entry_path = path.clone().join(name.parse()?);
					self.check_out_inner(artifact, existing_artifact.as_ref(), &entry_path)
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
		existing_artifact: Option<&tg::Artifact>,
		file: &tg::File,
		path: &tg::Path,
	) -> Result<()> {
		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				tg::util::rmrf(path).await?;
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Copy the blob to the path.
		let permit = self.file_descriptor_semaphore().acquire().await;
		tokio::io::copy(
			&mut file.reader(self).await?,
			&mut tokio::fs::File::create(path)
				.await
				.wrap_err("Failed to create the file.")?,
		)
		.await
		.wrap_err("Failed to copy the blob.")?;
		drop(permit);

		// Make the file executable if necessary.
		if file.executable(self).await? {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(path, permissions)
				.await
				.wrap_err("Failed to set the permissions.")?;
		}

		// Check that the file has no references.
		if !file.references(self).await?.is_empty() {
			return Err(error!(r#"Cannot check out a file with references."#));
		}

		Ok(())
	}

	async fn check_out_symlink(
		&self,
		existing_artifact: Option<&tg::Artifact>,
		symlink: &tg::Symlink,
		path: &tg::Path,
	) -> Result<()> {
		// Handle an existing artifact at the path.
		match &existing_artifact {
			// If there is an existing file system object at the path, then remove it and continue.
			Some(_) => {
				tg::util::rmrf(&path).await?;
			},

			// If there is no file system object at this path, then continue.
			None => (),
		};

		// Render the target.
		if symlink.artifact(self).await?.is_some() {
			return Err(error!(
				r#"Cannot check out a symlink which contains an artifact."#
			));
		}
		let target = symlink
			.path(self)
			.await?
			.as_ref()
			.cloned()
			.unwrap_or_default();

		// Create the symlink.
		tokio::fs::symlink(target, path)
			.await
			.wrap_err("Failed to create the symlink")?;

		Ok(())
	}
}
