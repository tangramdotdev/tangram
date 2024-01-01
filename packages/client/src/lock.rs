pub use self::data::Data;
use crate::{id, object, return_error, Dependency, Directory, Error, Handle, Result, WrapErr};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::Display;
use futures::{stream::FuturesUnordered, TryStreamExt};
use itertools::Itertools;
use std::{collections::BTreeMap, sync::Arc};

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
pub struct Lock {
	state: Arc<std::sync::RwLock<State>>,
}

type State = object::State<Id, Object>;

#[derive(Clone, Debug, Default)]
pub struct Object {
	pub dependencies: BTreeMap<Dependency, Entry>,
}

#[derive(Clone, Debug)]
pub struct Entry {
	pub package: Directory,
	pub lock: Lock,
}

pub mod data {
	use crate::{directory, Dependency};
	use serde_with::{serde_as, DisplayFromStr};
	use std::collections::BTreeMap;

	#[serde_as]
	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		#[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
		pub dependencies: BTreeMap<Dependency, Entry>,
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Entry {
		pub package: directory::Id,
		pub lock: super::Id,
	}
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(id::Kind::Lock, bytes))
	}
}

impl Lock {
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
		let dependencies = object
			.dependencies
			.iter()
			.map(|(dependency, entry)| async move {
				Ok::<_, Error>((dependency.clone(), entry.data(tg).await?))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(Data { dependencies })
	}
}

impl Lock {
	pub async fn dependencies(&self, tg: &dyn Handle) -> Result<&BTreeMap<Dependency, Entry>> {
		Ok(&self.object(tg).await?.dependencies)
	}
}

impl Entry {
	pub async fn data(&self, tg: &dyn Handle) -> Result<data::Entry> {
		Ok(data::Entry {
			package: self.package.id(tg).await?.clone(),
			lock: self.lock.id(tg).await?.clone(),
		})
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
		self.dependencies
			.values()
			.flat_map(|entry| [entry.package.clone().into(), entry.lock.clone().into()])
			.collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let dependencies = data
			.dependencies
			.into_iter()
			.map(|(dependency, entry)| Ok::<_, Error>((dependency, entry.try_into()?)))
			.try_collect()?;
		Ok(Self { dependencies })
	}
}

impl TryFrom<data::Entry> for Entry {
	type Error = Error;

	fn try_from(value: data::Entry) -> std::result::Result<Self, Self::Error> {
		Ok(Self {
			package: Directory::with_id(value.package),
			lock: Lock::with_id(value.lock),
		})
	}
}

impl Default for Lock {
	fn default() -> Self {
		Self::with_object(Object::default())
	}
}

impl std::fmt::Display for Lock {
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
		if value.kind() != id::Kind::Lock {
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
