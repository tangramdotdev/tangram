use crate as tg;
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use std::{
	collections::HashSet,
	sync::{Arc, Mutex},
};
use tangram_either::Either;

pub mod archive;
pub mod bundle;
pub mod checkin;
pub mod checkout;
pub mod checksum;
pub mod extract;

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
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
#[try_unwrap(ref)]
#[unwrap(ref)]
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
pub enum Object {
	/// A directory.
	Directory(Arc<tg::directory::Object>),

	/// A file.
	File(Arc<tg::file::Object>),

	/// A symlink.
	Symlink(Arc<tg::symlink::Object>),
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

impl Artifact {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Directory(id) => Self::Directory(tg::Directory::with_id(id)),
			Id::File(id) => Self::File(tg::File::with_id(id)),
			Id::Symlink(id) => Self::Symlink(tg::Symlink::with_id(id)),
		}
	}

	pub async fn id<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => Ok(directory.id(handle).await?.into()),
			Self::File(file) => Ok(file.id(handle).await?.into()),
			Self::Symlink(symlink) => Ok(Box::pin(symlink.id(handle)).await?.into()),
		}
	}

	pub async fn object<H>(&self, handle: &H) -> tg::Result<Object>
	where
		H: tg::Handle,
	{
		self.load(handle).await
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Object>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => directory.load(handle).await.map(Into::into),
			Self::File(file) => file.load(handle).await.map(Into::into),
			Self::Symlink(symlink) => symlink.load(handle).await.map(Into::into),
		}
	}

	pub fn unload(&self) {
		match self {
			Self::Directory(directory) => directory.unload(),
			Self::File(file) => file.unload(),
			Self::Symlink(symlink) => symlink.unload(),
		}
	}

	pub async fn store<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => directory.store(handle).await.map(Into::into),
			Self::File(file) => file.store(handle).await.map(Into::into),
			Self::Symlink(symlink) => symlink.store(handle).await.map(Into::into),
		}
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => Ok(directory.data(handle).await?.into()),
			Self::File(file) => Ok(file.data(handle).await?.into()),
			Self::Symlink(symlink) => Ok(symlink.data(handle).await?.into()),
		}
	}
}

impl Artifact {
	/// Collect an artifact's dependencies.
	pub async fn dependencies<H>(&self, handle: &H) -> tg::Result<Vec<Self>>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => Ok(directory
				.entries(handle)
				.await?
				.values()
				.map(|artifact| artifact.dependencies(handle))
				.collect::<FuturesOrdered<_>>()
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.flatten()
				.collect()),

			Self::File(file) => Ok(file
				.dependencies(handle)
				.await?
				.as_ref()
				.map(|dependencies| match dependencies {
					Either::Left(dependencies) => dependencies
						.iter()
						.filter_map(|object| tg::Artifact::try_from(object.clone()).ok())
						.collect(),
					Either::Right(dependencies) => dependencies
						.values()
						.filter_map(|object| tg::Artifact::try_from(object.clone()).ok())
						.collect(),
				})
				.unwrap_or_default()),

			Self::Symlink(symlink) => Ok(symlink
				.artifact(handle)
				.await?
				.clone()
				.map(Into::into)
				.into_iter()
				.collect()),
		}
	}

	/// Collect an artifact's recursive dependencies.
	pub async fn recursive_dependencies<H>(
		&self,
		handle: &H,
	) -> tg::Result<HashSet<Id, fnv::FnvBuildHasher>>
	where
		H: tg::Handle,
	{
		async fn recursive_dependencies_inner<H>(
			handle: &H,
			artifact: &tg::Artifact,
			output: Arc<Mutex<HashSet<Id, std::hash::BuildHasherDefault<fnv::FnvHasher>>>>,
		) -> tg::Result<()>
		where
			H: tg::Handle,
		{
			let dependencies = artifact.dependencies(handle).await?;
			dependencies
				.iter()
				.map(|artifact| recursive_dependencies_inner(handle, artifact, output.clone()))
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;
			let dependencies = dependencies
				.into_iter()
				.map(|artifact| async move { artifact.id(handle).await })
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
			output.lock().unwrap().extend(dependencies);
			Ok(())
		}
		let output = Arc::new(Mutex::new(HashSet::default()));
		recursive_dependencies_inner(handle, self, output.clone()).await?;
		Ok(Arc::into_inner(output).unwrap().into_inner().unwrap())
	}
}

impl Data {
	pub fn id(&self) -> tg::Result<Id> {
		match self {
			Self::Directory(artifact) => Ok(artifact.id()?.into()),
			Self::File(artifact) => Ok(artifact.id()?.into()),
			Self::Symlink(artifact) => Ok(artifact.id()?.into()),
		}
	}

	pub fn serialize(&self) -> tg::Result<Bytes> {
		match self {
			Self::Directory(artifact) => artifact.serialize(),
			Self::File(artifact) => artifact.serialize(),
			Self::Symlink(artifact) => artifact.serialize(),
		}
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

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
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

impl From<String> for Artifact {
	fn from(value: String) -> Self {
		tg::File::from(value).into()
	}
}

impl From<&str> for Artifact {
	fn from(value: &str) -> Self {
		tg::File::from(value).into()
	}
}
