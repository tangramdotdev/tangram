use crate::{artifact, id, object, Artifact, Error, Handle, Result, Symlink};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::Display;
use futures::{stream::FuturesOrdered, TryStreamExt};
use std::{collections::BTreeMap, sync::Arc};
use tangram_error::error;

#[derive(
	Clone,
	Debug,
	Display,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(crate::Id);

#[derive(Clone, Debug)]
pub struct Directory {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	/// The directory's entries.
	pub entries: BTreeMap<String, Artifact>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	/// The directory's entries.
	pub entries: BTreeMap<String, artifact::Id>,
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(id::Kind::Directory, bytes))
	}
}

impl Directory {
	#[must_use]
	pub fn with_state(state: State) -> Self {
		Self {
			state: Arc::new(std::sync::RwLock::new(state)),
		}
	}

	#[must_use]
	pub fn state(&self) -> &std::sync::RwLock<State> {
		&self.state
	}

	#[must_use]
	pub fn with_id(id: Id) -> Self {
		let state = State::with_id(id);
		Self {
			state: Arc::new(std::sync::RwLock::new(state)),
		}
	}

	#[must_use]
	pub fn with_object(object: Object) -> Self {
		let state = State::with_object(object);
		Self {
			state: Arc::new(std::sync::RwLock::new(state)),
		}
	}

	pub async fn id(&self, tg: &dyn Handle) -> Result<&Id> {
		self.store(tg).await?;
		Ok(unsafe { &*(self.state.read().unwrap().id.as_ref().unwrap() as *const Id) })
	}

	pub async fn object(&self, tg: &dyn Handle) -> Result<&Object> {
		self.load(tg).await?;
		Ok(unsafe { &*(self.state.read().unwrap().object.as_ref().unwrap() as *const Object) })
	}

	pub async fn try_get_object(&self, tg: &dyn Handle) -> Result<Option<&Object>> {
		if !self.try_load(tg).await? {
			return Ok(None);
		}
		Ok(Some(unsafe {
			&*(self.state.read().unwrap().object.as_ref().unwrap() as *const Object)
		}))
	}

	pub async fn load(&self, tg: &dyn Handle) -> Result<()> {
		self.try_load(tg)
			.await?
			.then_some(())
			.ok_or_else(|| error!("failed to load the object"))
	}

	pub async fn try_load(&self, tg: &dyn Handle) -> Result<bool> {
		if self.state.read().unwrap().object.is_some() {
			return Ok(true);
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let Some(output) = tg.try_get_object(&id.clone().into()).await? else {
			return Ok(false);
		};
		let data = Data::deserialize(&output.bytes)
			.map_err(|source| error!(!source, "failed to deserialize the data"))?;
		let object = data.try_into()?;
		self.state.write().unwrap().object.replace(object);
		Ok(true)
	}

	pub async fn store(&self, tg: &dyn Handle) -> Result<()> {
		if self.state.read().unwrap().id.is_some() {
			return Ok(());
		}
		let data = self.data(tg).await?;
		let bytes = data.serialize()?;
		let id = Id::new(&bytes);
		let arg = object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		tg.put_object(&id.clone().into(), &arg)
			.await
			.map_err(|source| error!(!source, "failed to put the object"))?;
		self.state.write().unwrap().id.replace(id);
		Ok(())
	}

	#[async_recursion]
	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		let object = self.object(tg).await?;
		let entries = object
			.entries
			.iter()
			.map(|(name, artifact)| async move {
				Ok::<_, Error>((name.clone(), artifact.id(tg).await?))
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { entries })
	}
}

impl Directory {
	#[must_use]
	pub fn new(entries: BTreeMap<String, Artifact>) -> Self {
		Self::with_object(Object { entries })
	}

	pub async fn builder(&self, tg: &dyn Handle) -> Result<Builder> {
		Ok(Builder::new(self.object(tg).await?.entries.clone()))
	}

	pub async fn entries(&self, tg: &dyn Handle) -> Result<&BTreeMap<String, Artifact>> {
		Ok(&self.object(tg).await?.entries)
	}

	pub async fn get(&self, tg: &dyn Handle, path: &crate::Path) -> Result<Artifact> {
		let artifact = self
			.try_get(tg, path)
			.await?
			.ok_or_else(|| error!("failed to get the artifact"))?;
		Ok(artifact)
	}

	pub async fn try_get(&self, tg: &dyn Handle, path: &crate::Path) -> Result<Option<Artifact>> {
		// Track the current artifact.
		let mut artifact: Artifact = self.clone().into();

		// Track the current path.
		let mut current_path = crate::Path::default();

		// Handle each path component.
		for component in path.components() {
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
				.ok_or_else(|| error!("the path must contain only normal components"))?;
			let Some(entry) = directory.entries(tg).await?.get(name).cloned() else {
				return Ok(None);
			};

			// Get the artifact.
			artifact = entry;

			// If the artifact is a symlink, then resolve it.
			if let Artifact::Symlink(symlink) = &artifact {
				let from = Symlink::new(Some(self.clone().into()), Some(current_path.to_string()));
				match symlink
					.resolve_from(tg, Some(from))
					.await
					.map_err(|source| error!(!source, "failed to resolve the symlink"))?
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
	pub fn serialize(&self) -> Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize(bytes: &Bytes) -> Result<Self> {
		serde_json::from_reader(bytes.as_ref())
			.map_err(|source| error!(!source, "failed to deserialize the data"))
	}

	#[must_use]
	pub fn children(&self) -> Vec<object::Id> {
		self.entries.values().cloned().map(Into::into).collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let entries = data
			.entries
			.into_iter()
			.map(|(name, id)| (name, Artifact::with_id(id)))
			.collect();
		Ok(Self { entries })
	}
}

impl std::fmt::Display for Directory {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.state.read().unwrap().id().as_ref().unwrap())?;
		Ok(())
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = Error;

	fn try_from(value: crate::Id) -> Result<Self, Self::Error> {
		if value.kind() != id::Kind::Directory {
			return Err(error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for Id {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

#[derive(Clone, Debug, Default)]
pub struct Builder {
	entries: BTreeMap<String, Artifact>,
}

impl Builder {
	#[must_use]
	pub fn new(entries: BTreeMap<String, Artifact>) -> Self {
		Self { entries }
	}

	#[async_recursion]
	pub async fn add(
		mut self,
		tg: &dyn Handle,
		path: &crate::Path,
		artifact: Artifact,
	) -> Result<Self> {
		// Get the first component.
		let name = path
			.components()
			.iter()
			.nth(0)
			.ok_or_else(|| error!("expected the path to have at least one component"))?
			.try_unwrap_normal_ref()
			.ok()
			.ok_or_else(|| error!("the path must contain only normal components"))?;

		// Collect the trailing path.
		let trailing_path: crate::Path = path.components().iter().skip(1).cloned().collect();

		let artifact = if trailing_path.components().is_empty() {
			artifact
		} else {
			// Get or create a child directory.
			let builder = if let Some(child) = self.entries.get(name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| error!("expected the artifact to be a directory"))?
					.builder(tg)
					.await?
			} else {
				Self::default()
			};

			// Recurse.
			builder
				.add(tg, &trailing_path, artifact)
				.await?
				.build()
				.into()
		};

		// Add the artifact.
		self.entries.insert(name.clone(), artifact);

		Ok(self)
	}

	#[async_recursion]
	pub async fn remove(mut self, tg: &dyn Handle, path: &crate::Path) -> Result<Self> {
		// Get the first component.
		let name = path
			.components()
			.iter()
			.nth(0)
			.ok_or_else(|| error!("expected the path to have at least one component"))?
			.try_unwrap_normal_ref()
			.ok()
			.ok_or_else(|| error!("the path must contain only normal components"))?;

		// Collect the trailing path.
		let trailing_path: crate::Path = path.components().iter().skip(1).cloned().collect();

		if trailing_path.components().is_empty() {
			// Remove the entry.
			self.entries.remove(name);
		} else {
			// Get a child directory.
			let builder = if let Some(child) = self.entries.get(name) {
				child
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| error!("expected the artifact to be a directory"))?
					.builder(tg)
					.await?
			} else {
				return Err(error!(%path, "the path does not exist"));
			};

			// Recurse.
			let artifact = builder.remove(tg, &trailing_path).await?.build().into();

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
