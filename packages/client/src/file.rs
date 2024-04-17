use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use futures::{stream::FuturesOrdered, FutureExt as _, TryStreamExt as _};
use std::sync::Arc;

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

/// A file value.
#[derive(Clone, Debug)]
pub struct Object {
	/// The file's contents.
	pub contents: tg::Blob,

	/// Whether the file is executable.
	pub executable: bool,

	/// The file's references.
	pub references: Vec<tg::Artifact>,
}

/// File data.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub contents: tg::blob::Id,
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub executable: bool,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub references: Vec<tg::artifact::Id>,
}

/// The extended attributes of files.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Attributes {
	pub references: Vec<tg::artifact::Id>,
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

	pub async fn id<H>(&self, tg: &H, transaction: Option<&H::Transaction<'_>>) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		self.store(tg, transaction).await
	}

	pub async fn object(&self, tg: &impl tg::Handle) -> tg::Result<Arc<Object>> {
		self.load(tg).await
	}

	pub async fn load(&self, tg: &impl tg::Handle) -> tg::Result<Arc<Object>> {
		self.try_load(tg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self, tg: &impl tg::Handle) -> tg::Result<Option<Arc<Object>>> {
		if let Some(object) = self.state.read().unwrap().object.clone() {
			return Ok(Some(object));
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let Some(output) = tg.try_get_object(&id.into()).await? else {
			return Ok(None);
		};
		let data = Data::deserialize(&output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let object = Object::try_from(data)?;
		let object = Arc::new(object);
		self.state.write().unwrap().object.replace(object.clone());
		Ok(Some(object))
	}

	pub async fn store<H>(&self, tg: &H, transaction: Option<&H::Transaction<'_>>) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return Ok(id);
		}
		let data = self.data(tg, transaction).await?;
		let bytes = data.serialize()?;
		let id = Id::new(&bytes);
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		tg.put_object(&id.clone().into(), arg, transaction)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		self.state.write().unwrap().id.replace(id.clone());
		Ok(id)
	}

	pub async fn data<H>(
		&self,
		tg: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let object = self.object(tg).await?;
		let contents = object.contents.id(tg, transaction).await?.clone();
		let executable = object.executable;
		let references = object
			.references
			.iter()
			.map(|artifact| artifact.id(tg, transaction))
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
	pub fn new(contents: tg::Blob, executable: bool, references: Vec<tg::Artifact>) -> Self {
		Self::with_object(Object {
			contents,
			executable,
			references,
		})
	}

	#[must_use]
	pub fn builder(contents: tg::Blob) -> Builder {
		Builder::new(contents)
	}

	pub async fn contents(&self, tg: &impl tg::Handle) -> tg::Result<tg::Blob> {
		Ok(self.object(tg).await?.contents.clone())
	}

	pub async fn executable(&self, tg: &impl tg::Handle) -> tg::Result<bool> {
		Ok(self.object(tg).await?.executable)
	}

	pub async fn references(
		&self,
		tg: &impl tg::Handle,
	) -> tg::Result<impl std::ops::Deref<Target = Vec<tg::Artifact>>> {
		Ok(self.object(tg).await?.map(|object| &object.references))
	}

	pub async fn reader<H>(&self, tg: &H) -> tg::Result<tg::blob::Reader<H>>
	where
		H: tg::Handle,
	{
		self.contents(tg).await?.reader(tg).await
	}

	pub async fn size(&self, tg: &impl tg::Handle) -> tg::Result<u64> {
		self.contents(tg).await?.size(tg).await
	}

	pub async fn bytes(&self, tg: &impl tg::Handle) -> tg::Result<Vec<u8>> {
		self.contents(tg).await?.bytes(tg).await
	}

	pub async fn text(&self, tg: &impl tg::Handle) -> tg::Result<String> {
		self.contents(tg).await?.text(tg).await
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
		std::iter::once(self.contents.clone().into())
			.chain(self.references.iter().cloned().map(Into::into))
			.collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let contents = tg::Blob::with_id(data.contents);
		let executable = data.executable;
		let references = data
			.references
			.into_iter()
			.map(tg::Artifact::with_id)
			.collect();
		Ok(Self {
			contents,
			executable,
			references,
		})
	}
}

impl std::fmt::Display for File {
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
	executable: bool,
	references: Vec<tg::tg::Artifact>,
}

impl Builder {
	#[must_use]
	pub fn new(contents: tg::Blob) -> Self {
		Self {
			contents,
			executable: false,
			references: Vec::new(),
		}
	}

	#[must_use]
	pub fn contents(mut self, contents: tg::Blob) -> Self {
		self.contents = contents;
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: bool) -> Self {
		self.executable = executable;
		self
	}

	#[must_use]
	pub fn references(mut self, references: Vec<tg::Artifact>) -> Self {
		self.references = references;
		self
	}

	#[must_use]
	pub fn build(self) -> File {
		File::new(self.contents, self.executable, self.references)
	}
}
