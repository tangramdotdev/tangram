use crate::{artifact, blob, id, object, Artifact, Blob, Handle};
use bytes::Bytes;
use derive_more::Display;
use futures::{stream::FuturesOrdered, TryStreamExt};
use std::sync::Arc;
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
pub struct File {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = object::State<Id, Object>;

/// A file value.
#[derive(Clone, Debug)]
pub struct Object {
	/// The file's contents.
	pub contents: Blob,

	/// Whether the file is executable.
	pub executable: bool,

	/// The file's references.
	pub references: Vec<Artifact>,
}

/// File data.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub contents: blob::Id,
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub executable: bool,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub references: Vec<artifact::Id>,
}

/// The extended attributes of files.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Attributes {
	pub references: Vec<artifact::Id>,
}

/// The extended attributes key used to store the [`Attributes`].
pub const TANGRAM_FILE_XATTR_NAME: &str = "user.tangram";

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(id::Kind::File, bytes))
	}
}

impl File {
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

	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		let object = self.object(tg).await?;
		let contents = object.contents.id(tg).await?.clone();
		let executable = object.executable;
		let references = object
			.references
			.iter()
			.map(|artifact| artifact.id(tg))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data {
			contents,
			executable,
			references,
		})
	}
}

impl File {
	#[must_use]
	pub fn new(contents: Blob, executable: bool, references: Vec<Artifact>) -> Self {
		Self::with_object(Object {
			contents,
			executable,
			references,
		})
	}

	#[must_use]
	pub fn builder(contents: Blob) -> Builder {
		Builder::new(contents)
	}

	pub async fn contents(&self, tg: &dyn Handle) -> Result<&Blob> {
		Ok(&self.object(tg).await?.contents)
	}

	pub async fn executable(&self, tg: &dyn Handle) -> Result<bool> {
		Ok(self.object(tg).await?.executable)
	}

	pub async fn references(&self, tg: &dyn Handle) -> Result<&[Artifact]> {
		Ok(self.object(tg).await?.references.as_slice())
	}

	pub async fn reader(&self, tg: &dyn Handle) -> Result<blob::Reader> {
		self.contents(tg).await?.reader(tg).await
	}

	pub async fn size(&self, tg: &dyn Handle) -> Result<u64> {
		self.contents(tg).await?.size(tg).await
	}

	pub async fn bytes(&self, tg: &dyn Handle) -> Result<Vec<u8>> {
		self.contents(tg).await?.bytes(tg).await
	}

	pub async fn text(&self, tg: &dyn Handle) -> Result<String> {
		self.contents(tg).await?.text(tg).await
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
		std::iter::once(self.contents.clone().into())
			.chain(self.references.iter().cloned().map(Into::into))
			.collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let contents = Blob::with_id(data.contents);
		let executable = data.executable;
		let references = data.references.into_iter().map(Artifact::with_id).collect();
		Ok(Self {
			contents,
			executable,
			references,
		})
	}
}

impl std::fmt::Display for File {
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
		if value.kind() != id::Kind::File {
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

pub struct Builder {
	contents: Blob,
	executable: bool,
	references: Vec<Artifact>,
}

impl Builder {
	#[must_use]
	pub fn new(contents: Blob) -> Self {
		Self {
			contents,
			executable: false,
			references: Vec::new(),
		}
	}

	#[must_use]
	pub fn contents(mut self, contents: Blob) -> Self {
		self.contents = contents;
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: bool) -> Self {
		self.executable = executable;
		self
	}

	#[must_use]
	pub fn references(mut self, references: Vec<Artifact>) -> Self {
		self.references = references;
		self
	}

	#[must_use]
	pub fn build(self) -> File {
		File::new(self.contents, self.executable, self.references)
	}
}
