use crate::{
	util::http::{full, Incoming, Outgoing},
	Http, Server,
};
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use http_body_util::BodyExt as _;
use std::os::unix::fs::PermissionsExt as _;
use tangram_client as tg;
use tangram_database as db;
use tangram_database::prelude::*;

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> tg::Result<tg::artifact::CheckInOutput> {
		// If this is a checkin of a path in the checkouts directory, then retrieve the corresponding artifact.
		let checkouts_path = self.checkouts_path().try_into()?;
		if let Some(path) = arg.path.diff(&checkouts_path).filter(tg::Path::is_internal) {
			let id = path
				.components()
				.get(1)
				.ok_or_else(|| tg::error!("cannot check in the checkouts directory"))?
				.try_unwrap_normal_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?
				.parse::<tg::artifact::Id>()?;
			let path = tg::Path::with_components(path.components().iter().skip(2).cloned());
			if path.components().len() == 1 {
				return Ok(tg::artifact::CheckInOutput { id });
			}
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, &path).await?;
			let id = artifact.id(self, None).await?;
			return Ok(tg::artifact::CheckInOutput { id });
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// // Begin a transaction.
		// let transaction = connection
		// 	.transaction()
		// 	.boxed()
		// 	.await
		// 	.map_err(|source| tg::error!(!source, "failed to begin the transaction"))?;

		// Check in the artifact.
		let id = self
			.check_in_artifact_with_transaction(&arg.path, &connection)
			.await?;

		// // Commit the transaction.
		// transaction
		// 	.commit()
		// 	.await
		// 	.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// Create the output.
		let output = tg::artifact::CheckInOutput { id };

		Ok(output)
	}

	async fn check_in_artifact_with_transaction(
		&self,
		path: &tg::Path,
		transaction: &impl db::Query,
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
		transaction: &impl db::Query,
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
				let id = self
					.check_in_artifact_with_transaction(&path, transaction)
					.await?;
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
		self.put_object_with_transaction(&id.clone().into(), arg, transaction)
			.await?;

		Ok(id.into())
	}

	async fn check_in_file(
		&self,
		path: &tg::Path,
		metadata: &std::fs::Metadata,
		transaction: &impl db::Query,
	) -> tg::Result<tg::artifact::Id> {
		// Create the blob.
		let permit = self.file_descriptor_semaphore.acquire().await;
		let file = tokio::fs::File::open(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the file"))?;
		let contents = self
			.create_blob_with_transaction(file, transaction)
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
		self.put_object_with_transaction(&id.clone().into(), arg, transaction)
			.await?;

		Ok(id.into())
	}

	async fn check_in_symlink(
		&self,
		path: &tg::Path,
		_metadata: &std::fs::Metadata,
		transaction: &impl db::Query,
	) -> tg::Result<tg::artifact::Id> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(path).await.map_err(
			|source| tg::error!(!source, %path, r#"failed to read the symlink at path"#,),
		)?;

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::Template::unrender(artifacts_path, target)?;

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
		self.put_object_with_transaction(&id.clone().into(), arg, transaction)
			.await?;

		Ok(id.into())
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
}
