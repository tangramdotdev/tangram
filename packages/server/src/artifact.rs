use crate::{
	database::Transaction,
	tmp::Tmp,
	util::{
		fs::rmrf,
		http::{bad_request, full, Incoming, Outgoing},
	},
	Http, Server,
};
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use http_body_util::BodyExt as _;
use std::{collections::HashMap, os::unix::prelude::PermissionsExt as _, sync::Arc};
use tangram_client as tg;
use tangram_database::prelude::*;

mod archive;
mod bundle;
mod extract;

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> tg::Result<tg::artifact::CheckInOutput> {
		// Get a database connection.
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin the transaction"))?;

		// Check in the artifact.
		let id = self
			.check_in_artifact_inner(&arg.path, &transaction)
			.await?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// Create the output.
		let output = tg::artifact::CheckInOutput { id };

		Ok(output)
	}

	async fn check_in_artifact_inner(
		&self,
		path: &tg::Path,
		transaction: &Transaction<'_>,
	) -> tg::Result<tg::artifact::Id> {
		// Get the metadata for the file system object at the path.
		let metadata = tokio::fs::symlink_metadata(&path).await.map_err(
			|source| tg::error!(!source, %path, "failed to get the metadata for the path"),
		)?;

		// Call the appropriate function for the file system object at the path.
		if metadata.is_dir() {
			self.check_in_directory(path, &metadata, transaction)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to check in the directory"))
		} else if metadata.is_file() {
			self.check_in_file(path, &metadata, transaction)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to check in the file"))
		} else if metadata.is_symlink() {
			self.check_in_symlink(path, &metadata, transaction)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to check in the symlink"))
		} else {
			let file_type = metadata.file_type();
			Err(tg::error!(
				%path,
				?file_type,
				"invalid file type"
			))
		}
	}

	async fn check_in_directory(
		&self,
		path: &tg::Path,
		_metadata: &std::fs::Metadata,
		transaction: &Transaction<'_>,
	) -> tg::Result<tg::artifact::Id> {
		let names = {
			let _permit = self.file_descriptor_semaphore.acquire().await;
			let mut read_dir = tokio::fs::read_dir(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
			let mut names = Vec::new();
			while let Some(entry) = read_dir
				.next_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?
			{
				let name = entry
					.file_name()
					.to_str()
					.ok_or_else(|| {
						let name = entry.file_name();
						tg::error!(?name, "all file names must be valid UTF-8")
					})?
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
				let id = self.check_in_artifact_inner(&path, transaction).await?;
				Ok::<_, tg::Error>((name, id))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Create the directory.
		let data = tg::directory::Data { entries };
		let bytes = data.serialize()?;
		let id = tg::directory::Id::new(&bytes);
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		self.put_object(&id.clone().into(), arg, Some(transaction))
			.await?;
		Ok(id.into())
	}

	async fn check_in_file(
		&self,
		path: &tg::Path,
		metadata: &std::fs::Metadata,
		transaction: &Transaction<'_>,
	) -> tg::Result<tg::artifact::Id> {
		// Create the blob.
		let permit = self.file_descriptor_semaphore.acquire().await;
		let file = tokio::fs::File::open(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the file"))?;
		let contents = self
			.create_blob(file, Some(transaction))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the contents"))?;
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
		let bytes = data.serialize()?;
		let id = tg::file::Id::new(&bytes);
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		self.put_object(&id.clone().into(), arg, Some(transaction))
			.await?;

		Ok(id.into())
	}

	async fn check_in_symlink(
		&self,
		path: &tg::Path,
		_metadata: &std::fs::Metadata,
		transaction: &Transaction<'_>,
	) -> tg::Result<tg::artifact::Id> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(path).await.map_err(
			|source| tg::error!(!source, %path, r#"failed to read the symlink at path"#,),
		)?;

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let target = tg::Template::unrender(target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?;
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
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(artifact), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		let data = tg::symlink::Data { artifact, path };
		let bytes = data.serialize()?;
		let id = tg::symlink::Id::new(&bytes);
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		self.put_object(&id.clone().into(), arg, Some(transaction))
			.await?;

		Ok(id.into())
	}

	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> tg::Result<tg::artifact::CheckOutOutput> {
		let files = Arc::new(std::sync::RwLock::new(HashMap::default()));
		self.check_out_artifact_with_files(id, arg, files).await
	}

	async fn check_out_artifact_with_files(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
		files: Arc<std::sync::RwLock<HashMap<tg::file::Id, tg::Path>>>,
	) -> tg::Result<tg::artifact::CheckOutOutput> {
		let artifact = tg::Artifact::with_id(id.clone());
		if let Some(path) = arg.path {
			if !path.is_absolute() {
				return Err(tg::error!(%path, "the path must be absolute"));
			}
			let exists = tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to stat the path"))?;
			if exists && !arg.force {
				return Err(tg::error!(%path, "there is already a file system object at the path"));
			}
			if (path.as_ref() as &std::path::Path).starts_with(&self.path) {
				return Err(tg::error!(%path, "cannot check out into the server's directory"));
			}

			// Bundle the artifact.
			let artifact = artifact
				.bundle(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to bundle the artifact"))?;

			// Check in an existing artifact at the path.
			let existing_artifact = if exists {
				let arg = tg::artifact::CheckInArg { path: path.clone() };
				let output = self.check_in_artifact(arg).await?;
				Some(tg::Artifact::with_id(output.id))
			} else {
				None
			};

			// Perform the checkout.
			self.check_out_inner(
				&path,
				&artifact,
				existing_artifact.as_ref(),
				false,
				0,
				files,
			)
			.await?;

			Ok(tg::artifact::CheckOutOutput { path })
		} else {
			// Get the path in the checkouts directory.
			let id = artifact.id(self, None).await?;
			let path = self.checkouts_path().join(id.to_string()).try_into()?;

			// If there is already a file system object at the path, then return.
			if tokio::fs::try_exists(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to stat the path"))?
			{
				return Ok(tg::artifact::CheckOutOutput { path });
			}

			// Create a tmp.
			let tmp = Tmp::new(self);

			// Perform the checkout to the tmp.
			let existing = Arc::new(std::sync::RwLock::new(HashMap::default()));
			self.check_out_inner(
				&tmp.path.clone().try_into()?,
				&artifact,
				None,
				true,
				0,
				existing,
			)
			.await?;

			// Move the checkout to the checkouts directory.
			match tokio::fs::rename(&tmp, &path).await {
				Ok(()) => (),
				// If the entry in the checkouts directory exists, then remove the checkout to the tmp.
				Err(ref error)
					if matches!(error.raw_os_error(), Some(libc::ENOTEMPTY | libc::EEXIST)) =>
				{
					rmrf(&tmp).await?;
				},
				Err(source) => {
					return Err(
						tg::error!(!source, %tmp = tmp.path.display(), %path, "failed to move the checkout to the checkouts directory"),
					);
				},
			};

			Ok(tg::artifact::CheckOutOutput { path })
		}
	}

	async fn check_out_inner(
		&self,
		path: &tg::Path,
		artifact: &tg::Artifact,
		existing_artifact: Option<&tg::Artifact>,
		internal: bool,
		depth: usize,
		files: Arc<std::sync::RwLock<HashMap<tg::file::Id, tg::Path>>>,
	) -> tg::Result<()> {
		// If the artifact is the same as the existing artifact, then return.
		let id = artifact.id(self, None).await?;
		match existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if id == existing_artifact.id(self, None).await? {
					return Ok(());
				}
			},
		}

		// Call the appropriate function for the artifact's type.
		match artifact {
			tg::Artifact::Directory(directory) => {
				self.check_out_directory(
					path,
					directory,
					existing_artifact,
					internal,
					depth,
					files,
				)
				.await
				.map_err(
					|source| tg::error!(!source, %id, %path, "failed to check out the directory"),
				)?;
			},

			tg::Artifact::File(file) => {
				self.check_out_file(path, file, existing_artifact, internal, files)
					.await
					.map_err(
						|source| tg::error!(!source, %id, %path, "failed to check out the file"),
					)?;
			},

			tg::Artifact::Symlink(symlink) => {
				self.check_out_symlink(path, symlink, existing_artifact, internal, depth, files)
					.await
					.map_err(
						|source| tg::error!(!source, %id, %path, "failed to check out the symlink"),
					)?;
			},
		}

		// If this is an internal checkout, then set the file system object's modified time to the epoch.
		if internal {
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
		internal: bool,
		depth: usize,
		files: Arc<std::sync::RwLock<HashMap<tg::file::Id, tg::Path>>>,
	) -> tg::Result<()> {
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
						Ok::<_, tg::Error>(())
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
						internal,
						depth + 1,
						existing,
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
		internal: bool,
		files: Arc<std::sync::RwLock<HashMap<tg::file::Id, tg::Path>>>,
	) -> tg::Result<()> {
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
			.map_err(|source| tg::error!(!source, "failed to get the file's references"))?
			.iter()
			.map(|artifact| artifact.id(self, None))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's references"))?;
		if !references.is_empty() {
			if !internal {
				return Err(tg::error!(
					r#"cannot perform an external check out of a file with references"#
				));
			}
			references
				.iter()
				.map(|artifact| async {
					let arg = tg::artifact::CheckOutArg {
						path: None,
						force: false,
					};
					Box::pin(self.check_out_artifact_with_files(artifact, arg, files.clone()))
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

		// Check out the file, either from an existing path, an internal path, or from the file reader.
		let permit = self.file_descriptor_semaphore.acquire().await;
		let id = file.id(self, None).await?;
		let existing_path = files.read().unwrap().get(&id).cloned();
		let internal_path = self.checkouts_path().join(id.to_string());
		if let Some(existing_path) = existing_path {
			tokio::fs::copy(&existing_path, &path).await.map_err(
				|source| tg::error!(!source, %existing_path, %to = &path, %id, "failed to copy the file"),
			)?;
			drop(permit);
		} else if tokio::fs::copy(&internal_path, path).await.is_ok() {
			drop(permit);
		} else {
			// Create the file.
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
				xattr::set(path, tg::file::TANGRAM_FILE_XATTR_NAME, &attributes).map_err(
					|source| tg::error!(!source, "failed to set attributes as an xattr"),
				)?;
			}

			files.write().unwrap().insert(id.clone(), path.clone());
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
		files: Arc<std::sync::RwLock<HashMap<tg::file::Id, tg::Path>>>,
	) -> tg::Result<()> {
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
				return Err(tg::error!(
					r#"cannot perform an external check out of a symlink with an artifact"#
				));
			}
			let id = artifact.id(self, None).await?;
			let arg = tg::artifact::CheckOutArg::default();
			Box::pin(self.check_out_artifact_with_files(&id, arg, files)).await?;
		}

		// Render the target.
		let mut target = tg::Path::new();
		let artifact = symlink.artifact(self).await?;
		let path_ = symlink.path(self).await?;
		if let Some(artifact) = artifact.as_ref() {
			for _ in 0..depth {
				target.push(tg::path::Component::Parent);
			}
			target = target.join("../../.tangram/artifacts/".parse::<tg::Path>().unwrap());
			let id = &artifact.id(self, None).await?;
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

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_check_in_artifact_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		let output = self.handle.check_in_artifact(arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the response"))?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}

	pub async fn handle_check_out_artifact_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["artifacts", id, "checkout"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Check out the artifact.
		let output = self.handle.check_out_artifact(&id, arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the response"))?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
