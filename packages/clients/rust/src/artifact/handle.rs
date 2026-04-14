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
	pub fn with_pointer(pointer: tg::graph::Pointer) -> Self {
		match pointer.kind {
			tg::artifact::Kind::Directory => tg::Directory::with_pointer(pointer).into(),
			tg::artifact::Kind::File => tg::File::with_pointer(pointer).into(),
			tg::artifact::Kind::Symlink => tg::Symlink::with_pointer(pointer).into(),
		}
	}

	#[must_use]
	pub fn with_edge(edge: tg::graph::Edge<tg::Artifact>) -> Self {
		match edge {
			tg::graph::Edge::Pointer(pointer) => Self::with_pointer(pointer),
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

	pub async fn object(&self) -> tg::Result<Object> {
		let handle = tg::handle()?;
		self.object_with_handle(handle).await
	}

	pub async fn object_with_handle<H>(&self, handle: &H) -> tg::Result<Object>
	where
		H: tg::Handle,
	{
		self.load_with_handle(handle).await
	}

	pub async fn load(&self) -> tg::Result<Object> {
		let handle = tg::handle()?;
		self.load_with_handle(handle).await
	}

	pub async fn load_with_handle<H>(&self, handle: &H) -> tg::Result<Object>
	where
		H: tg::Handle,
	{
		self.load_with_arg_with_handle(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<Object> {
		let handle = tg::handle()?;
		self.load_with_arg_with_handle(handle, arg).await
	}

	pub async fn load_with_arg_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Object>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => directory
				.load_with_arg_with_handle(handle, arg)
				.await
				.map(Into::into),
			Self::File(file) => file
				.load_with_arg_with_handle(handle, arg)
				.await
				.map(Into::into),
			Self::Symlink(symlink) => symlink
				.load_with_arg_with_handle(handle, arg)
				.await
				.map(Into::into),
		}
	}

	pub fn unload(&self) {
		match self {
			Self::Directory(directory) => directory.unload(),
			Self::File(file) => file.unload(),
			Self::Symlink(symlink) => symlink.unload(),
		}
	}

	pub async fn store(&self) -> tg::Result<Id> {
		let handle = tg::handle()?;
		self.store_with_handle(handle).await
	}

	pub async fn store_with_handle<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => directory.store_with_handle(handle).await.map(Into::into),
			Self::File(file) => file.store_with_handle(handle).await.map(Into::into),
			Self::Symlink(symlink) => symlink.store_with_handle(handle).await.map(Into::into),
		}
	}

	pub async fn children(&self) -> tg::Result<Vec<tg::Object>> {
		let handle = tg::handle()?;
		self.children_with_handle(handle).await
	}

	pub async fn children_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.load_with_handle(handle).await?;
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
	pub async fn dependencies(&self) -> tg::Result<Vec<Self>> {
		let handle = tg::handle()?;
		self.dependencies_with_handle(handle).await
	}

	pub async fn dependencies_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<Self>>
	where
		H: tg::Handle,
	{
		match self {
			Self::Directory(directory) => Ok(directory
				.entries_with_handle(handle)
				.await?
				.values()
				.map(|artifact| artifact.dependencies_with_handle(handle))
				.collect::<FuturesOrdered<_>>()
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.flatten()
				.collect()),

			Self::File(file) => Ok(file
				.dependencies_with_handle(handle)
				.await?
				.into_values()
				.filter_map(|option| option?.0.item?.try_into().ok())
				.collect()),

			Self::Symlink(symlink) => Ok(symlink
				.artifact_with_handle(handle)
				.await?
				.clone()
				.into_iter()
				.collect()),
		}
	}

	/// Collect an artifact's recursive dependencies.
	pub async fn recursive_dependencies(&self) -> tg::Result<HashSet<Id, tg::id::BuildHasher>> {
		let handle = tg::handle()?;
		self.recursive_dependencies_with_handle(handle).await
	}

	pub async fn recursive_dependencies_with_handle<H>(
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
		let dependencies = self.dependencies_with_handle(handle).await?;
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
