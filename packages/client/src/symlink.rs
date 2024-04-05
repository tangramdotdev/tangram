use crate::{artifact, id, object, util::arc::Ext, Artifact, Handle, Path};
use bytes::Bytes;
use derive_more::Display;
use std::{str::FromStr, sync::Arc};
use tangram_error::{error, Error, Result};

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

pub type State = object::State<Id, Object>;

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

	pub async fn id(&self, tg: &dyn Handle) -> Result<Id> {
		self.store(tg).await
	}

	pub async fn object(&self, tg: &dyn Handle) -> Result<Arc<Object>> {
		self.load(tg).await
	}

	pub async fn load(&self, tg: &dyn Handle) -> Result<Arc<Object>> {
		self.try_load(tg)
			.await?
			.ok_or_else(|| error!("failed to load the object"))
	}

	pub async fn try_load(&self, tg: &dyn Handle) -> Result<Option<Arc<Object>>> {
		if let Some(object) = self.state.read().unwrap().object.clone() {
			return Ok(Some(object));
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let Some(output) = tg.try_get_object(&id.into()).await? else {
			return Ok(None);
		};
		let data = Data::deserialize(&output.bytes)
			.map_err(|source| error!(!source, "failed to deserialize the data"))?;
		let object = Object::try_from(data)?;
		let object = Arc::new(object);
		self.state.write().unwrap().object.replace(object.clone());
		Ok(Some(object))
	}

	pub async fn store(&self, tg: &dyn Handle) -> Result<Id> {
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return Ok(id);
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
		self.state.write().unwrap().id.replace(id.clone());
		Ok(id)
	}

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

	pub async fn artifact(&self, tg: &dyn Handle) -> Result<Option<Artifact>> {
		Ok(self.object(tg).await?.artifact.clone())
	}

	pub async fn path(
		&self,
		tg: &dyn Handle,
	) -> Result<impl std::ops::Deref<Target = Option<String>>> {
		Ok(self.object(tg).await?.map(|object| &object.path))
	}

	pub async fn resolve(&self, tg: &dyn Handle) -> Result<Option<Artifact>> {
		self.resolve_from(tg, None).await
	}

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
			from_artifact = Box::pin(symlink.resolve_from(tg, None)).await?;
		}
		let from_path = if let Some(from) = from {
			from.path(tg).await?.clone()
		} else {
			None
		};
		let mut artifact = self.artifact(tg).await?.clone();
		if let Some(artifact::Artifact::Symlink(symlink)) = artifact {
			artifact = Box::pin(symlink.resolve_from(tg, None)).await?;
		}
		let path = self.path(tg).await?.clone();

		if artifact.is_some() && from_artifact.is_some() {
			return Err(error!("expected no `from` value when `artifact` is set"));
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
			return Err(error!("expected a directory"));
		} else if artifact.is_some() && path.is_some() {
			if let Some(artifact::Artifact::Directory(directory)) = artifact {
				return directory
					.try_get(tg, &Path::from_str(&path.unwrap())?.normalize())
					.await;
			}
			return Err(error!("expected a directory"));
		}
		Err(error!("invalid symlink"))
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
	type Error = Error;

	fn try_from(value: crate::Id) -> Result<Self, Self::Error> {
		if value.kind() != id::Kind::Symlink {
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
