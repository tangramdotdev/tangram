use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use futures::{stream::FuturesOrdered, TryStreamExt as _};
use std::{collections::BTreeSet, sync::Arc};

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
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
	pub dependencies: Vec<tg::Artifact>,
	pub executable: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub contents: tg::blob::Id,

	#[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
	pub dependencies: BTreeSet<tg::artifact::Id>,

	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub executable: bool,
}

/// The extended attributes of files.
#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct Attributes {
	pub dependencies: BTreeSet<tg::artifact::Id>,
}

/// The extended attributes key used to store the [`Attributes`].
pub const TANGRAM_FILE_XATTR_NAME: &str = "user.tangram";

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
		let dependencies = object
			.dependencies
			.iter()
			.map(|artifact| artifact.id(handle))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		let executable = object.executable;
		Ok(Data {
			contents,
			dependencies,
			executable,
		})
	}
}

impl File {
	#[must_use]
	pub fn new(contents: tg::Blob, dependencies: Vec<tg::Artifact>, executable: bool) -> Self {
		Self::with_object(Object {
			contents,
			dependencies,
			executable,
		})
	}

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
	) -> tg::Result<impl std::ops::Deref<Target = Vec<tg::Artifact>>>
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
		std::iter::once(self.contents.clone().into())
			.chain(self.dependencies.iter().cloned().map(Into::into))
			.collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let contents = tg::Blob::with_id(data.contents);
		let dependencies = data
			.dependencies
			.into_iter()
			.map(tg::Artifact::with_id)
			.collect();
		let executable = data.executable;
		Ok(Self {
			contents,
			dependencies,
			executable,
		})
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		value.0
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

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

pub struct Builder {
	contents: tg::Blob,
	dependencies: Vec<tg::tg::Artifact>,
	executable: bool,
}

impl Builder {
	#[must_use]
	pub fn new(contents: impl Into<tg::Blob>) -> Self {
		Self {
			contents: contents.into(),
			executable: false,
			dependencies: Vec::new(),
		}
	}

	#[must_use]
	pub fn contents(mut self, contents: tg::Blob) -> Self {
		self.contents = contents;
		self
	}

	#[must_use]
	pub fn dependencies(mut self, dependencies: Vec<tg::Artifact>) -> Self {
		self.dependencies = dependencies;
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: bool) -> Self {
		self.executable = executable;
		self
	}

	#[must_use]
	pub fn build(self) -> File {
		File::new(self.contents, self.dependencies, self.executable)
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
