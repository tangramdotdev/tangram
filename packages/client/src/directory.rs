use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use futures::{stream::FuturesOrdered, FutureExt as _, TryStreamExt as _};
use std::{collections::BTreeMap, sync::Arc};

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
pub struct Directory {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	/// The directory's entries.
	pub entries: BTreeMap<String, tg::Artifact>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	/// The directory's entries.
	pub entries: BTreeMap<String, tg::artifact::Id>,
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Directory, bytes))
	}
}

impl Directory {
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
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
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
		let entries = object
			.entries
			.iter()
			.map(|(name, artifact)| async move {
				Ok::<_, tg::Error>((name.clone(), artifact.id(handle, transaction).await?))
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { entries })
	}
}

impl Directory {
	#[must_use]
	pub fn new(entries: BTreeMap<String, tg::Artifact>) -> Self {
		Self::with_object(Object { entries })
	}

	pub async fn builder<H>(&self, handle: &H) -> tg::Result<Builder>
	where
		H: tg::Handle,
	{
		Ok(Builder::new(self.object(handle).await?.entries.clone()))
	}

	pub async fn entries<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = BTreeMap<String, tg::Artifact>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.entries))
	}

	pub async fn get<H>(&self, handle: &H, path: &tg::Path) -> tg::Result<tg::Artifact>
	where
		H: tg::Handle,
	{
		let artifact = self
			.try_get(handle, path)
			.await?
			.ok_or_else(|| tg::error!("failed to get the artifact"))?;
		Ok(artifact)
	}

	pub async fn try_get<H>(&self, handle: &H, path: &tg::Path) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		let mut artifact: tg::Artifact = self.clone().into();

		// Track the current path.
		let mut current_path = tg::Path::new();

		// Handle each path component.
		for component in path.components().iter().skip(1) {
			// The artifact must be a directory.
			let Some(directory) = artifact.try_unwrap_directory_ref().ok() else {
				return Ok(None);
			};

			// Update the current path.
			current_path = current_path.join(component.clone());

			// Get the entry. If it doesn't exist, return `None`.
			let name = component
				.try_unwrap_normal_ref()
				.ok()
				.ok_or_else(|| tg::error!("the path must contain only normal components"))?;
			let Some(entry) = directory.entries(handle).await?.get(name).cloned() else {
				return Ok(None);
			};

			// Get the artifact.
			artifact = entry;

			// If the artifact is a symlink, then resolve it.
			if let tg::Artifact::Symlink(symlink) = &artifact {
				let from = tg::Symlink::new(Some(self.clone().into()), Some(current_path.clone()));
				match Box::pin(symlink.resolve_from(handle, Some(from)))
					.await
					.map_err(|source| tg::error!(!source, "failed to resolve the symlink"))?
				{
					Some(resolved) => artifact = resolved,
					None => return Ok(None),
				}
			}
		}

		Ok(Some(artifact))
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
	pub fn children(&self) -> Vec<tg::object::Id> {
		self.entries.values().cloned().map(Into::into).collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let entries = data
			.entries
			.into_iter()
			.map(|(name, id)| (name, tg::Artifact::with_id(id)))
			.collect();
		Ok(Self { entries })
	}
}

impl std::fmt::Display for Directory {
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
		if value.kind() != tg::id::Kind::Directory {
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

#[derive(Clone, Debug, Default)]
pub struct Builder {
	entries: BTreeMap<String, tg::Artifact>,
}

impl Builder {
	#[must_use]
	pub fn new(entries: BTreeMap<String, tg::Artifact>) -> Self {
		Self { entries }
	}

	pub async fn add<H>(
		mut self,
		handle: &H,
		path: &tg::Path,
		artifact: tg::Artifact,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Get the first component.
		let name = path
			.components()
			.iter()
			.nth(1)
			.ok_or_else(|| tg::error!("expected the path to have at least one component"))?
			.try_unwrap_normal_ref()
			.ok()
			.ok_or_else(|| tg::error!("the path must contain only normal components"))?;

		// Collect the trailing path.
		let trailing_path: tg::Path = path.components().iter().skip(2).cloned().collect();

		let artifact = if trailing_path.components().len() == 1 {
			artifact
		} else {
			// Get or create a child directory.
			let builder = if let Some(child) = self.entries.get(name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected the artifact to be a directory"))?
					.builder(handle)
					.await?
			} else {
				Self::default()
			};

			// Recurse.
			Box::pin(builder.add(handle, &trailing_path, artifact))
				.await?
				.build()
				.into()
		};

		// Add the artifact.
		self.entries.insert(name.clone(), artifact);

		Ok(self)
	}

	pub async fn remove<H>(mut self, handle: &H, path: &tg::Path) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Get the first component.
		let name = path
			.components()
			.iter()
			.nth(0)
			.ok_or_else(|| tg::error!("expected the path to have at least one component"))?
			.try_unwrap_normal_ref()
			.ok()
			.ok_or_else(|| tg::error!("the path must contain only normal components"))?;

		// Collect the trailing path.
		let trailing_path: tg::Path = path.components().iter().skip(1).cloned().collect();

		if trailing_path.components().is_empty() {
			// Remove the entry.
			self.entries.remove(name);
		} else {
			// Get a child directory.
			let builder = if let Some(child) = self.entries.get(name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected the artifact to be a directory"))?
					.builder(handle)
					.await?
			} else {
				return Err(tg::error!(%path, "the path does not exist"));
			};

			// Recurse.
			let artifact = Box::pin(builder.remove(handle, &trailing_path))
				.await?
				.build()
				.into();

			// Add the new artifact.
			self.entries.insert(name.clone(), artifact);
		};

		Ok(self)
	}

	#[must_use]
	pub fn build(self) -> Directory {
		Directory::new(self.entries)
	}
}
