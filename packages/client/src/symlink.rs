use crate::{artifact, id, object, Artifact, Error, Handle, Path, Result, WrapErr};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::Display;
use std::{str::FromStr, sync::Arc};
use tangram_error::return_error;

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
pub struct Symlink {
	state: Arc<std::sync::RwLock<State>>,
}

type State = object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub artifact: Option<Artifact>,
	pub path: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<artifact::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<String>,
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(id::Kind::Symlink, bytes))
	}
}

impl Symlink {
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
			.wrap_err("Failed to load the object.")
	}

	pub async fn try_load(&self, tg: &dyn Handle) -> Result<bool> {
		if self.state.read().unwrap().object.is_some() {
			return Ok(true);
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let Some(bytes) = tg.try_get_object(&id.clone().into()).await? else {
			return Ok(false);
		};
		let data = Data::deserialize(&bytes).wrap_err("Failed to deserialize the data.")?;
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
		let missing = tg
			.try_put_object(&id.clone().into(), &bytes)
			.await
			.wrap_err("Failed to put the object.")?;
		if !missing.is_empty() {
			return_error!("Expected all children to be stored.");
		}
		self.state.write().unwrap().id.replace(id);
		Ok(())
	}

	#[async_recursion]
	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		let object = self.object(tg).await?;
		let artifact = if let Some(artifact) = &object.artifact {
			Some(artifact.id(tg).await?)
		} else {
			None
		};
		let path = object.path.clone();
		Ok(Data { artifact, path })
	}
}

impl Symlink {
	#[must_use]
	pub fn new(artifact: Option<Artifact>, path: Option<String>) -> Self {
		Self::with_object(Object { artifact, path })
	}

	pub async fn artifact(&self, tg: &dyn Handle) -> Result<&Option<Artifact>> {
		let object = self.object(tg).await?;
		Ok(&object.artifact)
	}

	pub async fn path(&self, tg: &dyn Handle) -> Result<&Option<String>> {
		let object = self.object(tg).await?;
		Ok(&object.path)
	}

	pub async fn resolve(&self, tg: &dyn Handle) -> Result<Option<Artifact>> {
		self.resolve_from(tg, None).await
	}

	#[async_recursion]
	pub async fn resolve_from(
		&self,
		tg: &dyn Handle,
		from: Option<Self>,
	) -> Result<Option<Artifact>> {
		let mut from_artifact = if let Some(from) = &from {
			from.artifact(tg).await?.clone()
		} else {
			None
		};
		if let Some(artifact::Artifact::Symlink(symlink)) = from_artifact {
			from_artifact = symlink.resolve(tg).await?;
		}
		let from_path = if let Some(from) = from {
			from.path(tg).await?.clone()
		} else {
			None
		};
		let mut artifact = self.artifact(tg).await?.clone();
		if let Some(artifact::Artifact::Symlink(symlink)) = artifact {
			artifact = symlink.resolve(tg).await?;
		}
		let path = self.path(tg).await?.clone();

		if artifact.is_some() && from_artifact.is_some() {
			return_error!("Expected no `from` value when `artifact` is set.");
		}

		if artifact.is_some() && path.is_none() {
			return Ok(artifact);
		} else if artifact.is_none() && path.is_some() {
			if let Some(artifact::Artifact::Directory(directory)) = from_artifact {
				let path = Path::from_str(&from_path.unwrap_or(String::new()))?
					.join(Path::from_str("..")?)
					.join(Path::from_str(&path.unwrap())?)
					.normalize();
				return directory.try_get(tg, &path).await;
			}
			return_error!("Expected a directory.");
		} else if artifact.is_some() && path.is_some() {
			if let Some(artifact::Artifact::Directory(directory)) = artifact {
				return directory
					.try_get(tg, &Path::from_str(&path.unwrap())?.normalize())
					.await;
			}
			return_error!("Expected a directory.");
		}
		return_error!("Invalid symlink.")
	}
}

impl Data {
	pub fn serialize(&self) -> Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.wrap_err("Failed to serialize the data.")
	}

	pub fn deserialize(bytes: &Bytes) -> Result<Self> {
		serde_json::from_reader(bytes.as_ref()).wrap_err("Failed to deserialize the data.")
	}

	#[must_use]
	pub fn children(&self) -> Vec<object::Id> {
		self.artifact.iter().map(|id| id.clone().into()).collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let artifact = data.artifact.map(Artifact::with_id);
		let path = data.path;
		Ok(Self { artifact, path })
	}
}

impl std::fmt::Display for Symlink {
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
		if value.kind() != id::Kind::Symlink {
			return_error!("Invalid kind.");
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
