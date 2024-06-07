use crate::{database::Transaction, Server};
use bytes::Bytes;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive;
use std::{
	os::unix::fs::PermissionsExt as _,
	path::PathBuf,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};
use tangram_client as tg;
use tangram_database::{self as db, Connection as _, Database as _, Query as _, Transaction as _};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

struct CheckInOutput {
	id: tg::artifact::Id,
	weight: u64,
}

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<tg::artifact::checkin::Output> {
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
				return Ok(tg::artifact::checkin::Output { artifact: id });
			}
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, &path).await?;
			let id = artifact.id(self).await?;
			return Ok(tg::artifact::checkin::Output { artifact: id });
		}

		// Check in the artifact.
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database transaction"))?;

		let artifact = self
			.check_in_artifact_inner(&arg.path, &transaction)
			.await?
			.id;
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		// Create the output.
		let output = tg::artifact::checkin::Output { artifact };

		Ok(output)
	}

	async fn check_in_artifact_inner(
		&self,
		path: &tg::Path,
		transaction: &Transaction<'_>,
	) -> tg::Result<CheckInOutput> {
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
	) -> tg::Result<CheckInOutput> {
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
		let weight = Arc::new(AtomicU64::new(0));
		let entries = names
			.into_iter()
			.map(|name| async {
				let path = path.clone().join(&name);
				let output = self.check_in_artifact_inner(&path, transaction).await?;
				weight.fetch_add(output.weight, Ordering::Relaxed);
				Ok::<_, tg::Error>((name, output.id))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Create the directory data.
		let data = tg::directory::Data { entries };
		let bytes = data.serialize()?;
		let id = tg::artifact::Id::from(tg::directory::Id::new(&bytes));
		let weight = weight.fetch_add(bytes.len().to_u64().unwrap(), Ordering::Relaxed);

		// Put the directory into the database.
		self.put_artifact(transaction, &id, bytes, weight).await?;

		// Create the output.
		let output = CheckInOutput { id, weight };

		Ok(output)
	}

	async fn check_in_file(
		&self,
		path: &tg::Path,
		metadata: &std::fs::Metadata,
		transaction: &Transaction<'_>,
	) -> tg::Result<CheckInOutput> {
		// Create the blob without writing to disk/database.
		let _permit = self.file_descriptor_semaphore.acquire().await;
		let file = tokio::fs::File::open(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the file"))?;

		// Create the output.
		let output = self
			.create_or_store_blob(file, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the contents"))?;

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

		// Create the file data.
		let data = tg::file::Data {
			contents: output.blob.clone(),
			executable,
			references,
		};
		let bytes = data.serialize()?;
		let id = tg::artifact::Id::from(tg::file::Id::new(&bytes));

		// Copy the file to the checkouts directory.
		tokio::fs::copy(&path, self.checkouts_path().join(id.to_string()))
			.await
			.map_err(|source| tg::error!(!source, "failed to copy to the checkouts directory"))?;

		// Install a symlink in the blobs directory.
		let src = PathBuf::from("../checkouts").join(id.to_string());
		let dst = self.blobs_path().join(output.blob.to_string());
		tokio::fs::symlink(src, dst).await.map_err(|source| {
			tg::error!(
				!source,
				"failed to install symlink into checkouts directory"
			)
		})?;

		// Put the file into the database.
		self.put_artifact(transaction, &id, bytes, output.weight)
			.await?;

		// Create the output
		let output = CheckInOutput {
			id,
			weight: output.weight,
		};

		Ok(output)
	}

	async fn check_in_symlink(
		&self,
		path: &tg::Path,
		_metadata: &std::fs::Metadata,
		transaction: &Transaction<'_>,
	) -> tg::Result<CheckInOutput> {
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
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
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
		let artifact = if let Some(artifact) = artifact {
			Some(artifact.id(self).await?)
		} else {
			None
		};
		let symlink = tg::symlink::Data { artifact, path };
		let bytes = symlink.serialize()?;
		let id = tg::artifact::Id::from(tg::symlink::Id::new(&bytes));
		let weight = bytes.len().to_u64().unwrap();

		// Put the symlink into the database
		self.put_artifact(transaction, &id, bytes, weight).await?;

		// Create the output.
		let output = CheckInOutput { id, weight };

		Ok(output)
	}

	async fn put_artifact(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::artifact::Id,
		bytes: Bytes,
		weight: u64,
	) -> tg::Result<()> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, touched_at, count, weight)
				values ({p}1, {p}2, {p}3, {p}4, {p}5)
				on conflict (id) do update set touched_at = {p}3;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![id, bytes, now, weight, weight];
		transaction
			.execute(statement, params)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to put the artifact into the database")
			})?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_check_in_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.check_in_artifact(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
