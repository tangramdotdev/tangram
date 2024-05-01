use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	FutureExt as _, TryStreamExt as _,
};
use itertools::Itertools as _;
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
pub struct Target {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub host: String,
	pub executable: tg::Artifact,
	pub args: Vec<tg::Value>,
	pub env: BTreeMap<String, tg::Value>,
	pub lock: Option<tg::Lock>,
	pub checksum: Option<tg::Checksum>,
}

/// Target data.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub host: String,
	pub executable: tg::artifact::Id,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub args: Vec<tg::value::Data>,
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: BTreeMap<String, tg::value::Data>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lock: Option<tg::lock::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::Checksum>,
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Target, bytes))
	}
}

impl Target {
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
		let arg = tg::object::put::Arg {
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
		let host = object.host.clone();
		let executable = object.executable.id(handle, transaction).await?;
		let lock = if let Some(lock) = &object.lock {
			Some(lock.id(handle, transaction).await?)
		} else {
			None
		};
		let env = object
			.env
			.iter()
			.map(|(key, value)| async move {
				let key = key.clone();
				let value = value.data(handle, transaction).await?;
				Ok::<_, tg::Error>((key, value))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		let args = object
			.args
			.iter()
			.map(|value| value.data(handle, transaction))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		let checksum = object.checksum.clone();
		Ok(Data {
			host,
			executable,
			args,
			env,
			lock,
			checksum,
		})
	}
}

impl Target {
	pub async fn host<H>(&self, handle: &H) -> tg::Result<impl std::ops::Deref<Target = String>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.host))
	}

	pub async fn executable<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = tg::Artifact>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.executable))
	}

	pub async fn args<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Vec<tg::Value>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.args))
	}

	pub async fn env<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = BTreeMap<String, tg::Value>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.env))
	}

	pub async fn lock<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Option<tg::Lock>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.lock))
	}

	pub async fn checksum<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Option<tg::Checksum>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.checksum))
	}

	pub async fn package<H>(&self, handle: &H) -> tg::Result<Option<tg::Directory>>
	where
		H: tg::Handle,
	{
		let object = &self.object(handle).await?;
		let tg::Artifact::Symlink(symlink) = &object.executable else {
			return Ok(None);
		};
		let Some(artifact) = symlink.artifact(handle).await? else {
			return Ok(None);
		};
		let Some(directory) = artifact.try_unwrap_directory_ref().ok() else {
			return Ok(None);
		};
		Ok(Some(directory.clone()))
	}

	pub async fn build<H>(&self, handle: &H, arg: tg::build::create::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let build = tg::Build::new(handle, arg.clone()).await?;
		let outcome = build.outcome(handle).await?;
		match outcome {
			tg::build::Outcome::Canceled => Err(tg::error!("the build was canceled")),
			tg::build::Outcome::Failed(error) => Err(error),
			tg::build::Outcome::Succeeded(value) => Ok(value),
		}
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
		std::iter::empty()
			.chain(std::iter::once(self.executable.clone().into()))
			.chain(self.args.iter().flat_map(tg::value::Data::children))
			.chain(self.env.values().flat_map(tg::value::Data::children))
			.chain(self.lock.clone().map(Into::into))
			.collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let host = data.host;
		let executable = tg::Artifact::with_id(data.executable);
		let args = data.args.into_iter().map(TryInto::try_into).try_collect()?;
		let env = data
			.env
			.into_iter()
			.map(|(key, data)| Ok::<_, tg::Error>((key, data.try_into()?)))
			.try_collect()?;
		let lock = data.lock.map(tg::Lock::with_id);
		let checksum = data.checksum;
		Ok(Self {
			host,
			executable,
			args,
			env,
			lock,
			checksum,
		})
	}
}

impl std::fmt::Display for Target {
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
		if value.kind() != tg::id::Kind::Target {
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

#[derive(Clone, Debug)]
pub struct Builder {
	host: String,
	executable: tg::Artifact,
	args: Vec<tg::Value>,
	env: BTreeMap<String, tg::Value>,
	lock: Option<tg::Lock>,
	checksum: Option<tg::Checksum>,
}

impl Builder {
	#[must_use]
	pub fn new(host: String, executable: tg::Artifact) -> Self {
		Self {
			host,
			executable,
			args: Vec::new(),
			env: BTreeMap::new(),
			lock: None,
			checksum: None,
		}
	}

	#[must_use]
	pub fn host(mut self, host: String) -> Self {
		self.host = host;
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: tg::Artifact) -> Self {
		self.executable = executable;
		self
	}

	#[must_use]
	pub fn args(mut self, args: Vec<tg::Value>) -> Self {
		self.args = args;
		self
	}

	#[must_use]
	pub fn env(mut self, env: BTreeMap<String, tg::Value>) -> Self {
		self.env = env;
		self
	}

	#[must_use]
	pub fn lock(mut self, lock: tg::Lock) -> Self {
		self.lock = Some(lock);
		self
	}

	#[must_use]
	pub fn checksum(mut self, checksum: Option<tg::Checksum>) -> Self {
		self.checksum = checksum;
		self
	}

	#[must_use]
	pub fn build(self) -> Target {
		Target::with_object(Object {
			host: self.host,
			executable: self.executable,
			args: self.args,
			env: self.env,
			lock: self.lock,
			checksum: self.checksum,
		})
	}
}
