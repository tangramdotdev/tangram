use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use futures::FutureExt as _;
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
pub struct Symlink {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub artifact: Option<tg::Artifact>,
	pub path: Option<tg::Path>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::artifact::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Symlink, bytes))
	}
}

impl Symlink {
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

	pub async fn id<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		self.store(handle, transaction).await
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

	pub async fn store<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return Ok(id);
		}
		let data = self.data(handle, transaction).await?;
		let bytes = data.serialize()?;
		let id = Id::new(&bytes);
		let arg = tg::object::put::Arg { bytes };
		handle
			.put_object(&id.clone().into(), arg, transaction)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		self.state.write().unwrap().id.replace(id.clone());
		Ok(id)
	}

	pub async fn data<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let artifact = if let Some(artifact) = &object.artifact {
			Some(artifact.id(handle, transaction).await?)
		} else {
			None
		};
		let path = object.path.clone();
		Ok(Data { artifact, path })
	}
}

impl Symlink {
	#[must_use]
	pub fn new(artifact: Option<tg::Artifact>, path: Option<tg::Path>) -> Self {
		Self::with_object(Object { artifact, path })
	}

	pub async fn artifact<H>(&self, handle: &H) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.artifact.clone())
	}

	pub async fn path<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Option<tg::Path>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.path))
	}

	pub async fn resolve<H>(&self, handle: &H) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		self.resolve_from(handle, None).await
	}

	pub async fn resolve_from<H>(
		&self,
		handle: &H,
		from: Option<Self>,
	) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		let mut from_artifact = if let Some(from) = &from {
			from.artifact(handle).await?.clone()
		} else {
			None
		};
		if let Some(tg::artifact::Artifact::Symlink(symlink)) = from_artifact {
			from_artifact = Box::pin(symlink.resolve_from(handle, None)).await?;
		}
		let from_path = if let Some(from) = from {
			from.path(handle).await?.clone()
		} else {
			None
		};
		let mut artifact = self.artifact(handle).await?.clone();
		if let Some(tg::artifact::Artifact::Symlink(symlink)) = artifact {
			artifact = Box::pin(symlink.resolve_from(handle, None)).await?;
		}
		let path = self.path(handle).await?.clone();
		if artifact.is_some() && from_artifact.is_some() {
			return Err(tg::error!(
				"expected no `from` value when `artifact` is set"
			));
		}
		if artifact.is_some() && path.is_none() {
			return Ok(artifact);
		} else if artifact.is_none() && path.is_some() {
			if let Some(tg::artifact::Artifact::Directory(directory)) = from_artifact {
				let path = from_path
					.unwrap_or_default()
					.join(tg::Path::with_components([tg::path::Component::Parent]))
					.join(path.unwrap_or_default())
					.normalize();
				return directory.try_get(handle, &path).await;
			}
			return Err(tg::error!("expected a directory"));
		} else if artifact.is_some() && path.is_some() {
			if let Some(tg::artifact::Artifact::Directory(directory)) = artifact {
				return directory.try_get(handle, &path.unwrap_or_default()).await;
			}
			return Err(tg::error!("expected a directory"));
		}
		Err(tg::error!("invalid symlink"))
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
		self.artifact.iter().map(|id| id.clone().into()).collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let artifact = data.artifact.map(tg::Artifact::with_id);
		let path = data.path;
		Ok(Self { artifact, path })
	}
}

impl std::fmt::Display for Symlink {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(id) = self.state.read().unwrap().id().as_ref() {
			write!(f, "{id}")?;
		} else {
			write!(f, "<unstored>")?;
		}
		Ok(())
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
		if value.kind() != tg::id::Kind::Symlink {
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
