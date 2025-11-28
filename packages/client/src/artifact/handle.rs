use {
	super::{Id, Object},
	crate::prelude::*,
	futures::{
		TryStreamExt as _,
		stream::{FuturesOrdered, FuturesUnordered},
	},
	std::{
		collections::HashSet,
		sync::{Arc, Mutex},
	},
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

	#[must_use]
	pub fn with_reference(reference: tg::graph::Reference) -> Self {
		match reference.kind {
			tg::artifact::Kind::Directory => tg::Directory::with_reference(reference).into(),
			tg::artifact::Kind::File => tg::File::with_reference(reference).into(),
			tg::artifact::Kind::Symlink => tg::Symlink::with_reference(reference).into(),
		}
	}

	#[must_use]
	pub fn with_edge(edge: tg::graph::Edge<tg::Artifact>) -> Self {
		match edge {
			tg::graph::Edge::Reference(reference) => Self::with_reference(reference),
			tg::graph::Edge::Object(artifact) => artifact,
		}
	}

	#[must_use]
	pub fn id(&self) -> Id {
		match self {
			Self::Directory(directory) => directory.id().into(),
			Self::File(file) => file.id().into(),
			Self::Symlink(symlink) => symlink.id().into(),
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

	#[must_use]
	pub fn kind(&self) -> tg::artifact::Kind {
		match self {
			Self::Directory(_) => tg::artifact::Kind::Directory,
			Self::File(_) => tg::artifact::Kind::File,
			Self::Symlink(_) => tg::artifact::Kind::Symlink,
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
				.filter_map(|referent| referent?.item.try_into().ok())
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
	) -> tg::Result<HashSet<Id, tg::id::BuildHasher>>
	where
		H: tg::Handle,
	{
		let output = Arc::new(Mutex::new(HashSet::default()));
		self.recursive_dependencies_inner(handle, output.clone())
			.await?;
		let output = Arc::into_inner(output).unwrap().into_inner().unwrap();
		Ok(output)
	}

	async fn recursive_dependencies_inner<H>(
		&self,
		handle: &H,
		output: Arc<Mutex<HashSet<Id, tg::id::BuildHasher>>>,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let dependencies = self.dependencies(handle).await?;
		dependencies
			.iter()
			.map(|artifact| artifact.recursive_dependencies_inner(handle, output.clone()))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;
		let dependencies = dependencies
			.into_iter()
			.map(|artifact| artifact.id())
			.collect::<Vec<_>>();
		output.lock().unwrap().extend(dependencies);
		Ok(())
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
