use crate::{
	artifact, build, id, lock, object, value, Artifact, Build, Checksum, Directory, Error, Handle,
	Lock, Result, System, User, Value, WrapErr,
};
use bytes::Bytes;
use derive_more::Display;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt,
};
use itertools::Itertools;
use std::{collections::BTreeMap, sync::Arc};
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
pub struct Target {
	state: Arc<std::sync::RwLock<State>>,
}

type State = object::State<Id, Object>;

/// A target object.
#[derive(Clone, Debug)]
pub struct Object {
	/// The system to build the target on.
	pub host: System,

	/// The target's executable.
	pub executable: Artifact,

	/// The target's lock.
	pub lock: Option<Lock>,

	/// The target's name.
	pub name: Option<String>,

	/// The target's env.
	pub env: BTreeMap<String, Value>,

	/// The target's args.
	pub args: Vec<Value>,

	/// If a checksum of the target's output is provided, then the target will have access to the network.
	pub checksum: Option<Checksum>,
}

/// Target data.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub host: System,
	pub executable: artifact::Id,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub lock: Option<lock::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: BTreeMap<String, value::Data>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub args: Vec<value::Data>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<Checksum>,
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_hashed(id::Kind::Target, bytes))
	}
}

impl Target {
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
		tg.try_put_object(&id.clone().into(), &bytes)
			.await
			.wrap_err("Failed to put the object.")?
			.ok()
			.wrap_err("Expected all children to be stored.")?;
		self.state.write().unwrap().id.replace(id);
		Ok(())
	}

	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		let object = self.object(tg).await?;
		let host = object.host.clone();
		let executable = object.executable.id(tg).await?;
		let lock = if let Some(lock) = &object.lock {
			Some(lock.id(tg).await?.clone())
		} else {
			None
		};
		let name = object.name.clone();
		let env = object
			.env
			.iter()
			.map(|(key, value)| async move {
				let key = key.clone();
				let value = value.data(tg).await?;
				Ok::<_, Error>((key, value))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		let args = object
			.args
			.iter()
			.map(|value| value.data(tg))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		let checksum = object.checksum.clone();
		Ok(Data {
			host,
			executable,
			lock,
			name,
			env,
			args,
			checksum,
		})
	}
}

impl Target {
	pub async fn host(&self, tg: &dyn Handle) -> Result<&System> {
		Ok(&self.object(tg).await?.host)
	}

	pub async fn executable(&self, tg: &dyn Handle) -> Result<&Artifact> {
		Ok(&self.object(tg).await?.executable)
	}

	pub async fn lock(&self, tg: &dyn Handle) -> Result<&Option<Lock>> {
		Ok(&self.object(tg).await?.lock)
	}

	pub async fn name(&self, tg: &dyn Handle) -> Result<&Option<String>> {
		Ok(&self.object(tg).await?.name)
	}

	pub async fn env(&self, tg: &dyn Handle) -> Result<&BTreeMap<String, Value>> {
		Ok(&self.object(tg).await?.env)
	}

	pub async fn args(&self, tg: &dyn Handle) -> Result<&Vec<Value>> {
		Ok(&self.object(tg).await?.args)
	}

	pub async fn checksum(&self, tg: &dyn Handle) -> Result<&Option<Checksum>> {
		Ok(&self.object(tg).await?.checksum)
	}

	pub async fn package(&self, tg: &dyn Handle) -> Result<Option<&Directory>> {
		let object = &self.object(tg).await?;
		let Artifact::Symlink(symlink) = &object.executable else {
			return Ok(None);
		};
		let Some(artifact) = symlink.artifact(tg).await? else {
			return Ok(None);
		};
		let Some(directory) = artifact.try_unwrap_directory_ref().ok() else {
			return Ok(None);
		};
		Ok(Some(directory))
	}

	pub async fn build(
		&self,
		tg: &dyn Handle,
		user: Option<&User>,
		depth: u64,
		retry: build::Retry,
	) -> Result<Build> {
		let target_id = self.id(tg).await?;
		let build_id = tg
			.get_or_create_build_for_target(user, target_id, depth, retry)
			.await?;
		let build = Build::with_id(build_id);
		Ok(build)
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
		std::iter::empty()
			.chain(std::iter::once(self.executable.clone().into()))
			.chain(self.lock.clone().map(Into::into))
			.chain(self.env.values().flat_map(value::Data::children))
			.chain(self.args.iter().flat_map(value::Data::children))
			.collect()
	}
}

impl TryFrom<Data> for Object {
	type Error = Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		Ok(Self {
			host: data.host,
			executable: Artifact::with_id(data.executable),
			lock: data.lock.map(Lock::with_id),
			name: data.name,
			env: data
				.env
				.into_iter()
				.map(|(key, data)| Ok::<_, Error>((key, data.try_into()?)))
				.try_collect()?,
			args: data.args.into_iter().map(TryInto::try_into).try_collect()?,
			checksum: data.checksum,
		})
	}
}

impl std::fmt::Display for Target {
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
		if value.kind() != id::Kind::Target {
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

#[derive(Clone, Debug)]
pub struct Builder {
	host: System,
	executable: Artifact,
	lock: Option<Lock>,
	name: Option<String>,
	env: BTreeMap<String, Value>,
	args: Vec<Value>,
	checksum: Option<Checksum>,
}

impl Builder {
	#[must_use]
	pub fn new(host: System, executable: Artifact) -> Self {
		Self {
			host,
			executable,
			lock: None,
			name: None,
			env: BTreeMap::new(),
			args: Vec::new(),
			checksum: None,
		}
	}

	#[must_use]
	pub fn host(mut self, host: System) -> Self {
		self.host = host;
		self
	}

	#[must_use]
	pub fn executable(mut self, executable: Artifact) -> Self {
		self.executable = executable;
		self
	}

	#[must_use]
	pub fn lock(mut self, lock: Lock) -> Self {
		self.lock = Some(lock);
		self
	}

	#[must_use]
	pub fn name(mut self, name: String) -> Self {
		self.name = Some(name);
		self
	}

	#[must_use]
	pub fn env(mut self, env: BTreeMap<String, Value>) -> Self {
		self.env = env;
		self
	}

	#[must_use]
	pub fn args(mut self, args: Vec<Value>) -> Self {
		self.args = args;
		self
	}

	#[must_use]
	pub fn checksum(mut self, checksum: Option<Checksum>) -> Self {
		self.checksum = checksum;
		self
	}

	#[must_use]
	pub fn build(self) -> Target {
		Target::with_object(Object {
			lock: self.lock,
			host: self.host,
			executable: self.executable,
			name: self.name,
			env: self.env,
			args: self.args,
			checksum: self.checksum,
		})
	}
}
