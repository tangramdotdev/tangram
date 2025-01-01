use super::{Data, Id, Object};
use crate as tg;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use std::{
	collections::HashSet,
	sync::{Arc, Mutex},
};

/// An artifact.
#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
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
			Self::Directory(directory) => Ok(Box::pin(directory.id(handle)).await?.into()),
			Self::File(file) => Ok(Box::pin(file.id(handle)).await?.into()),
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

	pub async fn children<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.load(handle).await?;
		Ok(object.children())
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
				.into_values()
				.filter_map(|referent| referent.item.try_into().ok())
				.collect()),

			Self::Symlink(symlink) => Ok(symlink
				.artifact(handle)
				.await?
				.clone()
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
				.try_collect::<()>()
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
		let output = Arc::into_inner(output).unwrap().into_inner().unwrap();
		Ok(output)
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
