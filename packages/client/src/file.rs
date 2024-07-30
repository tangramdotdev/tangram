use crate::{self as tg, util::arc::Ext as _, util::serde::is_false};
use bytes::Bytes;
use either::Either;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

pub use self::data::Data;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::Into,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(crate::Id);

#[derive(Clone, Debug)]
pub struct File {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub contents: tg::Blob,
	pub dependencies: Option<Dependencies>,
	pub executable: bool,
	pub metadata: Option<tg::value::Map>,
}

#[derive(Clone, Debug)]
pub enum Dependencies {
	Set(Vec<tg::Object>),
	Map(BTreeMap<tg::Reference, tg::Object>),
	Lock(tg::Lock, usize),
}

pub mod data {
	use super::*;

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		pub contents: tg::blob::Id,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub dependencies: Option<Dependencies>,

		#[serde(default, skip_serializing_if = "is_false")]
		pub executable: bool,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub metadata: Option<tg::value::data::Map>,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
	pub enum Dependencies {
		Set(BTreeSet<tg::object::Id>),
		Map(BTreeMap<tg::Reference, tg::object::Id>),
		Lock(tg::lock::Id, usize),
	}
}

/// The extended attributes key used to store the dependencies.
pub const TANGRAM_FILE_DEPENDENCIES_XATTR_NAME: &str = "user.tangram.dependencies";

/// The extended attributes key used to store the metadata.
pub const TANGRAM_FILE_METADATA_XATTR_NAME: &str = "user.tangram.metadata";

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::File, bytes))
	}
}

impl File {
	#[must_use]
	pub fn with_state(state: State) -> Self {
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	#[must_use]
	pub fn state(&self) -> &std::sync::RwLock<State> {
		&self.state
	}

	#[must_use]
	pub fn with_id(id: Id) -> Self {
		let state = State::with_id(id);
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<Object>>) -> Self {
		let state = State::with_object(object);
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	pub async fn id<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		self.store(handle).await
	}

	pub async fn object<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.load(handle).await
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load<H>(&self, handle: &H) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		if let Some(object) = self.state.read().unwrap().object.clone() {
			return Ok(Some(object));
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let Some(output) = handle.try_get_object(&id.into()).await? else {
			return Ok(None);
		};
		let data = Data::deserialize(&output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let object = Object::try_from(data)?;
		let object = Arc::new(object);
		self.state.write().unwrap().object.replace(object.clone());
		Ok(Some(object))
	}

	pub fn unload(&self) {
		self.state.write().unwrap().object.take();
	}

	pub async fn store<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return Ok(id);
		}
		let data = self.data(handle).await?;
		let bytes = data.serialize()?;
		let id = Id::new(&bytes);
		let arg = tg::object::put::Arg { bytes };
		handle
			.put_object(&id.clone().into(), arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		self.state.write().unwrap().id.replace(id.clone());
		Ok(id)
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let contents = object.contents.id(handle).await?.clone();
		let dependencies = if let Some(dependencies) = &object.dependencies {
			match dependencies {
				tg::file::Dependencies::Set(dependencies) => {
					Some(tg::file::data::Dependencies::Set(
						dependencies
							.iter()
							.map(|object| object.id(handle))
							.collect::<FuturesUnordered<_>>()
							.try_collect()
							.await?,
					))
				},
				tg::file::Dependencies::Map(dependencies) => {
					Some(tg::file::data::Dependencies::Map(
						dependencies
							.iter()
							.map(|(reference, object)| async {
								Ok::<_, tg::Error>((reference.clone(), object.id(handle).await?))
							})
							.collect::<FuturesUnordered<_>>()
							.try_collect()
							.await?,
					))
				},
				tg::file::Dependencies::Lock(lock, index) => Some(
					tg::file::data::Dependencies::Lock(lock.id(handle).await?, *index),
				),
			}
		} else {
			None
		};
		let executable = object.executable;
		let metadata = if let Some(metadata) = &object.metadata {
			Some(
				metadata
					.iter()
					.map(|(key, value)| async {
						Ok::<_, tg::Error>((key.clone(), value.data(handle).await?))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?,
			)
		} else {
			None
		};
		Ok(Data {
			contents,
			dependencies,
			executable,
			metadata,
		})
	}
}

impl File {
	#[must_use]
	pub fn builder(contents: impl Into<tg::Blob>) -> Builder {
		Builder::new(contents)
	}

	#[must_use]
	pub fn with_contents(contents: impl Into<tg::Blob>) -> Self {
		Self::builder(contents).build()
	}

	pub async fn contents<H>(&self, handle: &H) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.contents.clone())
	}

	pub async fn executable<H>(&self, handle: &H) -> tg::Result<bool>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.executable)
	}

	pub async fn dependencies<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Option<Dependencies>>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object(handle)
			.await?
			.map(|object| &object.dependencies))
	}

	pub async fn reader<H>(&self, handle: &H) -> tg::Result<tg::blob::Reader<H>>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.reader(handle).await
	}

	pub async fn size<H>(&self, handle: &H) -> tg::Result<u64>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.size(handle).await
	}

	pub async fn bytes<H>(&self, handle: &H) -> tg::Result<Vec<u8>>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.bytes(handle).await
	}

	pub async fn text<H>(&self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.text(handle).await
	}
}

impl File {
	pub async fn get_dependency<H>(
		&self,
		handle: &H,
		reference: &tg::Reference,
	) -> tg::Result<tg::Object>
	where
		H: tg::Handle,
	{
		self.try_get_dependency(handle, reference)
			.await
			.and_then(|option| {
				option.ok_or_else(|| tg::error!(%reference, "failed to find the dependency"))
			})
	}

	pub async fn try_get_dependency<H>(
		&self,
		handle: &H,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let Some(dependencies) = object.dependencies.as_ref() else {
			return Ok(None);
		};
		match dependencies {
			Dependencies::Set(_) => Ok(None),
			Dependencies::Map(dependencies) => Ok(dependencies.get(reference).cloned()),
			Dependencies::Lock(lock, index) => {
				let object = lock.object(handle).await?;
				let node = object
					.nodes
					.get(*index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let Some(dependency) = node
					.dependencies
					.as_ref()
					.and_then(|dependencies| dependencies.get(reference))
				else {
					return Ok(None);
				};
				let dependency = match dependency {
					Either::Left(index) => {
						let node = object
							.nodes
							.get(*index)
							.ok_or_else(|| tg::error!("invalid index"))?;
						let Some(dependency) = &node.object else {
							return Ok(None);
						};
						dependency.clone()
					},
					Either::Right(object) => object.clone(),
				};
				Ok(Some(dependency))
			},
		}
	}

	pub async fn metadata<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Option<tg::value::Map>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.metadata))
	}
}

impl Data {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize(bytes: &Bytes) -> tg::Result<Self> {
		serde_json::from_reader(bytes.as_ref())
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		let contents = self.contents.clone().into();
		let dependencies = self
			.dependencies
			.as_ref()
			.map(self::data::Dependencies::children)
			.unwrap_or_default();
		std::iter::once(contents).chain(dependencies).collect()
	}
}

impl self::data::Dependencies {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			tg::file::data::Dependencies::Set(dependencies) => dependencies.clone(),
			tg::file::data::Dependencies::Map(dependencies) => {
				dependencies.values().cloned().collect()
			},
			tg::file::data::Dependencies::Lock(lock, _) => [lock.clone().into()].into(),
		}
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let contents = tg::Blob::with_id(data.contents);
		let dependencies = data.dependencies.map(|dependencies| match dependencies {
			tg::file::data::Dependencies::Set(dependencies) => tg::file::Dependencies::Set(
				dependencies.into_iter().map(tg::Object::with_id).collect(),
			),
			tg::file::data::Dependencies::Map(dependencies) => tg::file::Dependencies::Map(
				dependencies
					.into_iter()
					.map(|(reference, object)| (reference, tg::Object::with_id(object)))
					.collect(),
			),
			tg::file::data::Dependencies::Lock(lock, index) => {
				tg::file::Dependencies::Lock(tg::Lock::with_id(lock), index)
			},
		});
		let executable = data.executable;
		let metadata = data
			.metadata
			.map(|metadata| {
				metadata
					.into_iter()
					.map(|(key, value)| Ok::<_, tg::Error>((key, value.try_into()?)))
					.collect::<tg::Result<_>>()
			})
			.transpose()?;
		Ok(Self {
			contents,
			dependencies,
			executable,
			metadata,
		})
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::File {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl TryFrom<data::Dependencies> for Dependencies {
	type Error = tg::Error;

	fn try_from(value: data::Dependencies) -> Result<Self, Self::Error> {
		match value {
			data::Dependencies::Set(dependencies) => Ok(Dependencies::Set(
				dependencies.into_iter().map(tg::Object::with_id).collect(),
			)),
			data::Dependencies::Map(dependencies) => Ok(Dependencies::Map(
				dependencies
					.into_iter()
					.map(|(reference, object)| (reference, tg::Object::with_id(object)))
					.collect(),
			)),
			data::Dependencies::Lock(lock, node) => {
				Ok(Dependencies::Lock(tg::Lock::with_id(lock), node))
			},
		}
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

pub struct Builder {
	contents: tg::Blob,
	dependencies: Option<Dependencies>,
	executable: bool,
	metadata: Option<tg::value::Map>,
}

impl Builder {
	#[must_use]
	pub fn new(contents: impl Into<tg::Blob>) -> Self {
		Self {
			contents: contents.into(),
			dependencies: None,
			executable: false,
			metadata: None,
		}
	}

	#[must_use]
	pub fn contents(mut self, contents: impl Into<tg::Blob>) -> Self {
		self.contents = contents.into();
		self
	}

	#[must_use]
	pub fn dependencies(mut self, dependencies: impl Into<Option<tg::file::Dependencies>>) -> Self {
		self.dependencies = dependencies.into();
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: bool) -> Self {
		self.executable = executable;
		self
	}

	#[must_use]
	pub fn build(self) -> File {
		File::with_object(Object {
			contents: self.contents,
			dependencies: self.dependencies,
			executable: self.executable,
			metadata: self.metadata,
		})
	}
}

impl From<tg::Blob> for File {
	fn from(value: tg::Blob) -> Self {
		Self::with_contents(value)
	}
}

impl From<String> for File {
	fn from(value: String) -> Self {
		Self::with_contents(value)
	}
}

impl From<&str> for File {
	fn from(value: &str) -> Self {
		Self::with_contents(value)
	}
}
