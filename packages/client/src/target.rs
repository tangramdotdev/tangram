use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use itertools::Itertools as _;
use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

pub use self::{builder::Builder, data::Data};

pub mod build;
pub mod builder;

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
pub struct Target {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub args: tg::value::Array,
	pub checksum: Option<tg::Checksum>,
	pub env: tg::value::Map,
	pub executable: Option<Executable>,
	pub host: String,
}

#[derive(Clone, Debug, derive_more::From)]
pub enum Executable {
	Artifact(tg::Artifact),
	Module(tg::Module),
}

pub mod data {
	use super::*;

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		#[serde(default, skip_serializing_if = "Vec::is_empty")]
		pub args: tg::value::data::Array,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub checksum: Option<tg::Checksum>,

		#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
		pub env: tg::value::data::Map,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub executable: Option<tg::target::data::Executable>,

		pub host: String,
	}

	#[derive(Clone, Debug, derive_more::Display, serde::Deserialize, serde::Serialize)]
	#[serde(untagged)]
	pub enum Executable {
		Artifact(tg::artifact::Id),
		Module(tg::Module),
	}
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Target, bytes))
	}
}

impl Target {
	pub fn builder(host: impl Into<String>) -> Builder {
		Builder::new(host)
	}

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
		let args = object
			.args
			.iter()
			.map(|value| value.data(handle))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		let checksum = object.checksum.clone();
		let env = object
			.env
			.iter()
			.map(|(key, value)| async move {
				let key = key.clone();
				let value = value.data(handle).await?;
				Ok::<_, tg::Error>((key, value))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		let executable = if let Some(executable) = object.executable.as_ref() {
			let executable = match executable {
				Executable::Artifact(artifact) => {
					data::Executable::Artifact(artifact.id(handle).await?)
				},
				Executable::Module(module) => data::Executable::Module(module.clone()),
			};
			Some(executable)
		} else {
			None
		};
		let host = object.host.clone();
		Ok(Data {
			args,
			checksum,
			env,
			executable,
			host,
		})
	}
}

impl Target {
	pub async fn args<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Vec<tg::Value>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.args))
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

	pub async fn env<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = BTreeMap<String, tg::Value>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.env))
	}

	pub async fn executable<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = Option<Executable>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.executable))
	}

	pub async fn host<H>(&self, handle: &H) -> tg::Result<impl std::ops::Deref<Target = String>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.host))
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
		std::iter::empty()
			.chain(self.executable.iter().flat_map(data::Executable::children))
			.chain(self.args.iter().flat_map(tg::value::Data::children))
			.chain(self.env.values().flat_map(tg::value::Data::children))
			.collect()
	}
}

impl data::Executable {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			data::Executable::Artifact(id) => [id.clone().into()].into(),
			data::Executable::Module(module) => match &module.referent.item {
				tg::module::Item::Path(_) => [].into(),
				tg::module::Item::Object(id) => [id.clone()].into(),
			},
		}
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let args = data.args.into_iter().map(TryInto::try_into).try_collect()?;
		let checksum = data.checksum;
		let env = data
			.env
			.into_iter()
			.map(|(key, data)| Ok::<_, tg::Error>((key, data.try_into()?)))
			.try_collect()?;
		let executable = data.executable.map(TryInto::try_into).transpose()?;
		let host = data.host;
		Ok(Self {
			args,
			checksum,
			env,
			executable,
			host,
		})
	}
}

impl TryFrom<data::Executable> for Executable {
	type Error = tg::Error;

	fn try_from(data: data::Executable) -> std::result::Result<Self, Self::Error> {
		match data {
			data::Executable::Artifact(id) => Ok(Self::Artifact(tg::Artifact::with_id(id))),
			data::Executable::Module(module) => Ok(Self::Module(module)),
		}
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

impl From<tg::File> for Executable {
	fn from(value: tg::File) -> Self {
		Self::Artifact(value.into())
	}
}
