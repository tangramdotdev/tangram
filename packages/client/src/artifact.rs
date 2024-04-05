use crate as tg;
use crate::{
	checksum, directory, file, id, object, symlink,
	util::{fs::rmrf, http::full},
	Blob, Checksum, Client, Directory, File, Handle, Symlink, Template, Value,
};
use derive_more::{From, TryInto, TryUnwrap};
use futures::stream::{FuturesOrdered, FuturesUnordered, TryStreamExt};
use http_body_util::BodyExt;
use std::{
	collections::{HashSet, VecDeque},
	os::unix::fs::PermissionsExt,
};
use tangram_error::{error, Error, Result};

/// An artifact kind.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Directory,
	File,
	Symlink,
}

/// An artifact ID.
#[derive(
	Clone,
	Debug,
	Eq,
	From,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	TryInto,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub enum Id {
	/// A directory ID.
	Directory(directory::Id),

	/// A file ID.
	File(file::Id),

	/// A symlink ID.
	Symlink(symlink::Id),
}

/// An artifact.
#[derive(Clone, Debug, From, TryUnwrap)]
#[try_unwrap(ref)]
pub enum Artifact {
	/// A directory.
	Directory(Directory),

	/// A file.
	File(File),

	/// A symlink.
	Symlink(Symlink),
}

#[derive(Clone, Debug, From, TryUnwrap)]
#[try_unwrap(ref)]
pub enum Data {
	/// A directory.
	Directory(directory::Data),

	/// A file.
	File(file::Data),

	/// A symlink.
	Symlink(symlink::Data),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CheckInArg {
	pub path: crate::Path,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CheckInOutput {
	pub id: Id,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct CheckOutArg {
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub force: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<crate::Path>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CheckOutOutput {
	pub path: crate::Path,
}

impl Artifact {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Directory(id) => Self::Directory(Directory::with_id(id)),
			Id::File(id) => Self::File(File::with_id(id)),
			Id::Symlink(id) => Self::Symlink(Symlink::with_id(id)),
		}
	}

	pub async fn id(&self, tg: &dyn Handle) -> Result<Id> {
		match self {
			Self::Directory(directory) => Ok(directory.id(tg).await?.into()),
			Self::File(file) => Ok(file.id(tg).await?.into()),
			Self::Symlink(symlink) => Ok(symlink.id(tg).await?.into()),
		}
	}

	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		match self {
			Self::Directory(directory) => Ok(directory.data(tg).await?.into()),
			Self::File(file) => Ok(file.data(tg).await?.into()),
			Self::Symlink(symlink) => Ok(symlink.data(tg).await?.into()),
		}
	}
}

impl Artifact {
	pub async fn check_in(tg: &dyn Handle, path: &crate::Path) -> Result<Self> {
		let arg = CheckInArg { path: path.clone() };
		let output = tg.check_in_artifact(arg).await?;
		let artifact = Self::with_id(output.id);
		Ok(artifact)
	}

	pub async fn check_out(&self, tg: &dyn Handle, arg: CheckOutArg) -> Result<CheckOutOutput> {
		let id = self.id(tg).await?;
		let output = tg.check_out_artifact(&id, arg).await?;
		Ok(output)
	}

	/// Compute an artifact's checksum.
	pub async fn checksum(
		&self,
		_tg: &dyn Handle,
		algorithm: checksum::Algorithm,
	) -> Result<Checksum> {
		match algorithm {
			checksum::Algorithm::Unsafe => Ok(Checksum::Unsafe),
			_ => Err(error!("not yet implemented")),
		}
	}

	/// Collect an artifact's references.
	pub async fn references(&self, tg: &dyn Handle) -> Result<Vec<Self>> {
		match self {
			Self::Directory(directory) => Ok(directory
				.entries(tg)
				.await?
				.values()
				.map(|artifact| artifact.references(tg))
				.collect::<FuturesOrdered<_>>()
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.flatten()
				.collect()),
			Self::File(file) => Ok(file.references(tg).await?.to_owned()),
			Self::Symlink(symlink) => Ok(symlink.artifact(tg).await?.clone().into_iter().collect()),
		}
	}

	/// Collect an artifact's recursive references.
	pub async fn recursive_references(
		&self,
		tg: &dyn Handle,
	) -> Result<HashSet<Id, fnv::FnvBuildHasher>> {
		// Create a queue of artifacts and a set of futures.
		let mut references = HashSet::default();
		let mut queue = VecDeque::new();
		let mut futures = FuturesUnordered::new();
		queue.push_back(self.clone());

		while let Some(artifact) = queue.pop_front() {
			// Add a request for the artifact's references to the futures.
			futures.push(async move { artifact.references(tg).await });

			// If the queue is empty, then get more artifacts from the futures.
			if queue.is_empty() {
				// Get more artifacts from the futures.
				if let Some(artifacts) = futures.try_next().await? {
					// Handle each artifact.
					for artifact in artifacts {
						// Insert the artifact into the set of references.
						let inserted = references.insert(artifact.id(tg).await?);

						// If the artifact was new, then add it to the queue.
						if inserted {
							queue.push_back(artifact);
						}
					}
				}
			}
		}

		Ok(references)
	}
}

impl Artifact {
	pub async fn with_path(tg: &dyn Handle, path: &crate::Path) -> Result<Id> {
		// Get the metadata for the file system object at the path.
		let metadata = tokio::fs::symlink_metadata(path)
			.await
			.map_err(|source| error!(!source, %path, "failed to get the metadata for the path"))?;

		// Call the appropriate function for the file system object at the path.
		let id = if metadata.is_dir() {
			Self::with_path_directory(tg, path, &metadata).await?
		} else if metadata.is_file() {
			Self::with_path_file(tg, path, &metadata).await?
		} else if metadata.is_symlink() {
			Self::with_path_symlink(tg, path, &metadata).await?
		} else {
			let file_type = metadata.file_type();
			return Err(error!(?file_type, "invalid fie type"));
		};

		Ok(id)
	}

	async fn with_path_directory(
		tg: &dyn Handle,
		path: &crate::Path,
		_metadata: &std::fs::Metadata,
	) -> Result<Id> {
		// Read the contents of the directory.
		let names = {
			let _permit = tg.file_descriptor_semaphore().acquire().await;
			let mut read_dir = tokio::fs::read_dir(path)
				.await
				.map_err(|source| error!(!source, "failed to read the directory"))?;
			let mut names = Vec::new();
			while let Some(entry) = read_dir
				.next_entry()
				.await
				.map_err(|source| error!(!source, "failed to get the directory entry"))?
			{
				let name = entry.file_name();
				let name = name
					.to_str()
					.ok_or_else(|| error!(?name, "all file names must be valid UTF-8"))?
					.to_owned();
				names.push(name);
			}
			names
		};

		// Recurse into the directory's entries.
		let entries = names
			.into_iter()
			.map(|name| async {
				let path = path.clone().join(name.clone());
				let artifact = Artifact::with_id(Self::with_path(tg, &path).await?);
				Ok::<_, Error>((name, artifact))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Create the directory.
		let directory = Directory::new(entries);
		let id = directory.id(tg).await?;

		Ok(id.into())
	}

	async fn with_path_file(
		tg: &dyn Handle,
		path: &crate::Path,
		metadata: &std::fs::Metadata,
	) -> Result<Id> {
		// Create the blob.
		let permit = tg.file_descriptor_semaphore().acquire().await;
		let file = tokio::fs::File::open(path)
			.await
			.map_err(|source| error!(!source, "failed to open the file"))?;
		let contents = Blob::with_reader(tg, file)
			.await
			.map_err(|source| error!(!source, "failed to create the contents"))?;
		drop(permit);

		// Determine if the file is executable.
		let executable = (metadata.permissions().mode() & 0o111) != 0;

		// Read the file's references from its xattrs.
		let attributes: Option<file::Attributes> = xattr::get(path, file::TANGRAM_FILE_XATTR_NAME)
			.ok()
			.flatten()
			.and_then(|attributes| serde_json::from_slice(&attributes).ok());
		let references = attributes
			.map(|attributes| attributes.references)
			.unwrap_or_default()
			.into_iter()
			.map(Artifact::with_id)
			.collect();

		// Create the file.
		let file = File::new(contents, executable, references);
		let id = file.id(tg).await?;

		Ok(id.into())
	}

	async fn with_path_symlink(
		tg: &dyn Handle,
		path: &crate::Path,
		_metadata: &std::fs::Metadata,
	) -> Result<Id> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(path)
			.await
			.map_err(|source| error!(!source, %path, "failed to read the symlink at path"))?;

		// Unrender the target.
		let target = target.to_str().ok_or_else(|| {
			let target = target.display();
			error!(?target, "the symlink target must be valid UTF-8")
		})?;
		let target = Template::unrender(target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| error!("invalid sylink"))?
				.clone();
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| error!("invalid sylink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| error!("invalid sylink"))?
				.clone();
			(Some(artifact), Some(path))
		} else {
			return Err(error!(%target, "invalid symlink"));
		};

		// Create the symlink.
		let symlink = Symlink::new(artifact, path);
		let id = symlink.id(tg).await?;

		Ok(id.into())
	}

	pub async fn check_out_local(tg: &dyn Handle, id: &Id, path: &crate::Path) -> Result<()> {
		let artifact = Self::with_id(id.clone());

		// Bundle the artifact.
		let artifact = artifact
			.bundle(tg)
			.await
			.map_err(|source| error!(!source, "failed to bundle the artifact"))?;

		// Check in an existing artifact at the path.
		let existing_artifact = if tokio::fs::try_exists(path)
			.await
			.map_err(|source| error!(!source, "failed to determine if the path exists"))?
		{
			Some(Artifact::with_id(Self::with_path(tg, path).await?))
		} else {
			None
		};

		// Check out the artifact recursively.
		Self::check_out_local_inner(tg, &artifact, existing_artifact.as_ref(), path).await?;

		Ok(())
	}

	async fn check_out_local_inner(
		tg: &dyn Handle,
		artifact: &Artifact,
		existing_artifact: Option<&Artifact>,
		path: &crate::Path,
	) -> Result<()> {
		// If the artifact is the same as the existing artifact, then return.
		let id = artifact.id(tg).await?;
		match existing_artifact {
			None => (),
			Some(existing_artifact) => {
				if id == existing_artifact.id(tg).await? {
					return Ok(());
				}
			},
		}

		// Call the appropriate function for the artifact's type.
		match artifact {
			Artifact::Directory(directory) => {
				Self::check_out_local_directory(tg, existing_artifact, directory, path)
					.await
					.map_err(
						|source| error!(!source, %id, %path, "failed to check out directory"),
					)?;
			},

			Artifact::File(file) => {
				Self::check_out_local_file(tg, existing_artifact, file, path)
					.await
					.map_err(|source| error!(!source, %id, %path, "failed to check out file"))?;
			},

			Artifact::Symlink(symlink) => {
				Self::check_out_local_symlink(tg, existing_artifact, symlink, path)
					.await
					.map_err(|source| error!(!source, %id, %path, "failed to check out symlink"))?;
			},
		}

		Ok(())
	}

	async fn check_out_local_directory(
		tg: &dyn Handle,
		existing_artifact: Option<&Artifact>,
		directory: &Directory,
		path: &crate::Path,
	) -> Result<()> {
		// Handle an existing artifact at the path.
		match existing_artifact {
			// If there is already a directory, then remove any extraneous entries.
			Some(Artifact::Directory(existing_directory)) => {
				existing_directory
					.entries(tg)
					.await?
					.iter()
					.map(|(name, _)| async move {
						if !directory.entries(tg).await?.contains_key(name) {
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
					.map_err(|source| error!(!source, "failed to create the directory"))?;
			},
			// If there is no artifact at this path, then create a directory.
			None => {
				tokio::fs::create_dir_all(path)
					.await
					.map_err(|source| error!(!source, "failed to create the directory"))?;
			},
		}

		// Recurse into the entries.
		directory
			.entries(tg)
			.await?
			.iter()
			.map(|(name, artifact)| {
				let existing_artifact = &existing_artifact;
				async move {
					// Retrieve an existing artifact.
					let existing_artifact = match existing_artifact {
						Some(Artifact::Directory(existing_directory)) => {
							let name = name
								.parse()
								.map_err(|source| error!(!source, "invalid entry name"))?;
							existing_directory.try_get(tg, &name).await?
						},
						_ => None,
					};

					// Recurse.
					let entry_path = path.clone().join(name);
					Self::check_out_local_inner(
						tg,
						artifact,
						existing_artifact.as_ref(),
						&entry_path,
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

	async fn check_out_local_file(
		tg: &dyn Handle,
		existing_artifact: Option<&Artifact>,
		file: &File,
		path: &crate::Path,
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

		// Copy the blob to the path.
		let permit = tg.file_descriptor_semaphore().acquire().await;
		tokio::io::copy(
			&mut file.reader(tg).await?,
			&mut tokio::fs::File::create(path)
				.await
				.map_err(|source| error!(!source, "failed to create the file"))?,
		)
		.await
		.map_err(|source| error!(!source, "failed to copy the blob"))?;
		drop(permit);

		// Make the file executable if necessary.
		if file.executable(tg).await? {
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(path, permissions)
				.await
				.map_err(|source| error!(!source, "failed to set the permissions"))?;
		}

		// Check that the file has no references.
		if !file.references(tg).await?.is_empty() {
			return Err(error!(r#"cannot check out a file with references"#));
		}

		Ok(())
	}

	async fn check_out_local_symlink(
		tg: &dyn Handle,
		existing_artifact: Option<&Artifact>,
		symlink: &Symlink,
		path: &crate::Path,
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

		// Render the target.
		if symlink.artifact(tg).await?.is_some() {
			return Err(error!(
				r#"cannot check out a symlink which contains an artifact"#
			));
		}
		let target = symlink.path(tg).await?.clone().unwrap_or_default();

		// Create the symlink.
		tokio::fs::symlink(target, path)
			.await
			.map_err(|source| error!(!source, "failed to create the symlink"))?;

		Ok(())
	}
}

impl Client {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput> {
		let method = http::Method::POST;
		let uri = "/artifacts/checkin";
		let body = serde_json::to_string(&arg)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> Result<tg::artifact::CheckOutOutput> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/checkout");
		let body = serde_json::to_string(&arg)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Directory(id) => write!(f, "{id}"),
			Self::File(id) => write!(f, "{id}"),
			Self::Symlink(id) => write!(f, "{id}"),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = Error;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Directory(id) => id.into(),
			Id::File(id) => id.into(),
			Id::Symlink(id) => id.into(),
		}
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = Error;

	fn try_from(value: crate::Id) -> Result<Self, Self::Error> {
		match value.kind() {
			id::Kind::Directory => Ok(Self::Directory(value.try_into()?)),
			id::Kind::File => Ok(Self::File(value.try_into()?)),
			id::Kind::Symlink => Ok(Self::Symlink(value.try_into()?)),
			kind => Err(error!(%kind, %value, "expected an artifact ID")),
		}
	}
}

impl From<Id> for object::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Directory(id) => id.into(),
			Id::File(id) => id.into(),
			Id::Symlink(id) => id.into(),
		}
	}
}

impl TryFrom<object::Id> for Id {
	type Error = Error;

	fn try_from(value: object::Id) -> Result<Self, Self::Error> {
		match value {
			object::Id::Directory(value) => Ok(value.into()),
			object::Id::File(value) => Ok(value.into()),
			object::Id::Symlink(value) => Ok(value.into()),
			value => Err(error!(%value, "expected an artifact ID")),
		}
	}
}

impl std::fmt::Display for Artifact {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Directory(directory) => write!(f, "{directory}"),
			Self::File(file) => write!(f, "{file}"),
			Self::Symlink(symlink) => write!(f, "{symlink}"),
		}
	}
}

impl From<Artifact> for object::Handle {
	fn from(value: Artifact) -> Self {
		match value {
			Artifact::Directory(directory) => Self::Directory(directory),
			Artifact::File(file) => Self::File(file),
			Artifact::Symlink(symlink) => Self::Symlink(symlink),
		}
	}
}

impl TryFrom<object::Handle> for Artifact {
	type Error = Error;

	fn try_from(value: object::Handle) -> Result<Self, Self::Error> {
		match value {
			object::Handle::Directory(directory) => Ok(Self::Directory(directory)),
			object::Handle::File(file) => Ok(Self::File(file)),
			object::Handle::Symlink(symlink) => Ok(Self::Symlink(symlink)),
			_ => Err(error!("expected an artifact")),
		}
	}
}

impl From<Artifact> for Value {
	fn from(value: Artifact) -> Self {
		object::Handle::from(value).into()
	}
}

impl TryFrom<Value> for Artifact {
	type Error = Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		object::Handle::try_from(value)
			.map_err(|source| error!(!source, "invalid value"))?
			.try_into()
	}
}
