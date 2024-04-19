use crate::{
	self as tg,
	util::http::{empty, full},
};
use futures::stream::{FuturesOrdered, FuturesUnordered, TryStreamExt as _};
use http_body_util::BodyExt as _;
use std::collections::{HashSet, VecDeque};

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
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	derive_more::TryInto,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub enum Id {
	/// A directory ID.
	Directory(tg::directory::Id),

	/// A file ID.
	File(tg::file::Id),

	/// A symlink ID.
	Symlink(tg::symlink::Id),
}

/// An artifact.
#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Artifact {
	/// A directory.
	Directory(tg::Directory),

	/// A file.
	File(tg::File),

	/// A symlink.
	Symlink(tg::Symlink),
}

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Data {
	/// A directory.
	Directory(tg::directory::Data),

	/// A file.
	File(tg::file::Data),

	/// A symlink.
	Symlink(tg::symlink::Data),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ArchiveArg {
	pub format: ArchiveFormat,
}

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum ArchiveFormat {
	Tar,
	Zip,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ArchiveOutput {
	pub id: tg::blob::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ExtractArg {
	pub blob: tg::blob::Id,
	pub format: tg::artifact::ArchiveFormat,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ExtractOutput {
	pub id: tg::artifact::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct BundleOutput {
	pub id: Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CheckInArg {
	pub path: tg::Path,
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
	pub path: Option<tg::Path>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CheckOutOutput {
	pub path: tg::Path,
}

impl Artifact {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Directory(id) => Self::Directory(tg::Directory::with_id(id)),
			Id::File(id) => Self::File(tg::File::with_id(id)),
			Id::Symlink(id) => Self::Symlink(tg::Symlink::with_id(id)),
		}
	}

	pub async fn id<H>(&self, handle: &H, transaction: Option<&H::Transaction<'_>>) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => Ok(directory.id(handle, transaction).await?.into()),
			Self::File(file) => Ok(file.id(handle, transaction).await?.into()),
			Self::Symlink(symlink) => Ok(Box::pin(symlink.id(handle, transaction)).await?.into()),
		}
	}

	pub async fn data<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => Ok(directory.data(handle, transaction).await?.into()),
			Self::File(file) => Ok(file.data(handle, transaction).await?.into()),
			Self::Symlink(symlink) => Ok(symlink.data(handle, transaction).await?.into()),
		}
	}
}

impl Artifact {
	pub async fn archive(
		&self,
		handle: &impl tg::Handle,
		format: ArchiveFormat,
	) -> tg::Result<tg::Blob> {
		let id = self.id(handle, None).await?;
		let arg = ArchiveArg { format };
		let output = handle.archive_artifact(&id, arg).await?;
		let blob = tg::Blob::with_id(output.id);
		Ok(blob)
	}

	pub async fn extract(
		handle: &impl tg::Handle,
		blob: &tg::Blob,
		format: ArchiveFormat,
	) -> tg::Result<Self> {
		let blob = blob.id(handle, None).await?;
		let arg = ExtractArg { blob, format };
		let output = handle.extract_artifact(arg).await?;
		let artifact = Self::with_id(output.id);
		Ok(artifact)
	}

	pub async fn bundle(&self, handle: &impl tg::Handle) -> tg::Result<Self> {
		let id = self.id(handle, None).await?;
		let output = handle.bundle_artifact(&id).await?;
		let artifact = Self::with_id(output.id);
		Ok(artifact)
	}

	pub async fn check_in(handle: &impl tg::Handle, path: &tg::Path) -> tg::Result<Self> {
		let arg = CheckInArg { path: path.clone() };
		let output = handle.check_in_artifact(arg).await?;
		let artifact = Self::with_id(output.id);
		Ok(artifact)
	}

	pub async fn check_out(
		&self,
		handle: &impl tg::Handle,
		arg: CheckOutArg,
	) -> tg::Result<CheckOutOutput> {
		let id = self.id(handle, None).await?;
		let output = handle.check_out_artifact(&id, arg).await?;
		Ok(output)
	}

	/// Compute an artifact's checksum.
	pub async fn checksum(
		&self,
		_handle: &impl tg::Handle,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		match algorithm {
			tg::checksum::Algorithm::Unsafe => Ok(tg::Checksum::Unsafe),
			_ => Err(tg::error!("unimplemented")),
		}
	}

	/// Collect an artifact's references.
	pub async fn references(&self, handle: &impl tg::Handle) -> tg::Result<Vec<Self>> {
		match self {
			Self::Directory(directory) => Ok(directory
				.entries(handle)
				.await?
				.values()
				.map(|artifact| artifact.references(handle))
				.collect::<FuturesOrdered<_>>()
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.flatten()
				.collect()),
			Self::File(file) => Ok(file.references(handle).await?.to_owned()),
			Self::Symlink(symlink) => Ok(symlink.artifact(handle).await?.clone().into_iter().collect()),
		}
	}

	/// Collect an artifact's recursive references.
	pub async fn recursive_references(
		&self,
		handle: &impl tg::Handle,
	) -> tg::Result<HashSet<Id, fnv::FnvBuildHasher>> {
		// Create a queue of artifacts and a set of futures.
		let mut references = HashSet::default();
		let mut queue = VecDeque::new();
		let mut futures = FuturesUnordered::new();
		queue.push_back(self.clone());

		while let Some(artifact) = queue.pop_front() {
			// Add a request for the artifact's references to the futures.
			futures.push(async move { artifact.references(handle).await });

			// If the queue is empty, then get more artifacts from the futures.
			if queue.is_empty() {
				// Get more artifacts from the futures.
				if let Some(artifacts) = futures.try_next().await? {
					// Handle each artifact.
					for artifact in artifacts {
						// Insert the artifact into the set of references.
						let inserted = references.insert(artifact.id(handle, None).await?);

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

impl tg::Client {
	pub async fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::ArchiveArg,
	) -> tg::Result<tg::artifact::ArchiveOutput> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/archive");
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn extract_artifact(
		&self,
		arg: tg::artifact::ExtractArg,
	) -> tg::Result<tg::artifact::ExtractOutput> {
		let method = http::Method::POST;
		let uri = "/artifacts/extract";
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> tg::Result<tg::artifact::BundleOutput> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/bundle");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> tg::Result<tg::artifact::CheckInOutput> {
		let method = http::Method::POST;
		let uri = "/artifacts/checkin";
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> tg::Result<tg::artifact::CheckOutOutput> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/checkout");
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
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
	type Err = tg::Error;

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
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Directory => Ok(Self::Directory(value.try_into()?)),
			tg::id::Kind::File => Ok(Self::File(value.try_into()?)),
			tg::id::Kind::Symlink => Ok(Self::Symlink(value.try_into()?)),
			kind => Err(tg::error!(%kind, %value, "expected an artifact ID")),
		}
	}
}

impl From<Id> for tg::object::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Directory(id) => id.into(),
			Id::File(id) => id.into(),
			Id::Symlink(id) => id.into(),
		}
	}
}

impl TryFrom<tg::object::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::object::Id) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Id::Directory(value) => Ok(value.into()),
			tg::object::Id::File(value) => Ok(value.into()),
			tg::object::Id::Symlink(value) => Ok(value.into()),
			value => Err(tg::error!(%value, "expected an artifact ID")),
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

impl From<Artifact> for tg::object::Handle {
	fn from(value: Artifact) -> Self {
		match value {
			Artifact::Directory(directory) => Self::Directory(directory),
			Artifact::File(file) => Self::File(file),
			Artifact::Symlink(symlink) => Self::Symlink(symlink),
		}
	}
}

impl TryFrom<tg::object::Handle> for Artifact {
	type Error = tg::Error;

	fn try_from(value: tg::object::Handle) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Handle::Directory(directory) => Ok(Self::Directory(directory)),
			tg::object::Handle::File(file) => Ok(Self::File(file)),
			tg::object::Handle::Symlink(symlink) => Ok(Self::Symlink(symlink)),
			_ => Err(tg::error!("expected an artifact")),
		}
	}
}

impl From<Artifact> for tg::Value {
	fn from(value: Artifact) -> Self {
		tg::object::Handle::from(value).into()
	}
}

impl TryFrom<tg::Value> for Artifact {
	type Error = tg::Error;

	fn try_from(value: tg::Value) -> tg::Result<Self, Self::Error> {
		tg::object::Handle::try_from(value)
			.map_err(|source| tg::error!(!source, "invalid value"))?
			.try_into()
	}
}

impl std::fmt::Display for ArchiveFormat {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Tar => {
				write!(f, "tar")?;
			},
			Self::Zip => {
				write!(f, "zip")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for ArchiveFormat {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"tar" => Ok(Self::Tar),
			"zip" => Ok(Self::Zip),
			extension => Err(tg::error!(%extension, "invalid format")),
		}
	}
}

impl From<ArchiveFormat> for String {
	fn from(value: ArchiveFormat) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for ArchiveFormat {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}
