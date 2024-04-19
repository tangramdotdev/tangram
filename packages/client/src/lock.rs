pub use self::data::Data;
use crate as tg;
use bytes::Bytes;
use either::Either;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	FutureExt as _, TryStreamExt as _,
};
use itertools::Itertools as _;
use std::{collections::BTreeMap, path::Path, sync::Arc};

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
pub struct Lock {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub root: usize,
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, Default)]
pub struct Node {
	pub dependencies: BTreeMap<tg::Dependency, Entry>,
}

#[derive(Clone, Debug)]
pub struct Entry {
	pub package: Option<tg::Directory>,
	pub lock: Either<usize, Lock>,
}

pub mod data {
	use super::Id;
	use crate as tg;
	use either::Either;
	use serde_with::{serde_as, DisplayFromStr};
	use std::collections::BTreeMap;

	#[serde_as]
	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		pub root: usize,
		pub nodes: Vec<Node>,
	}

	#[serde_as]
	#[derive(
		Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
	)]
	#[serde(transparent)]
	pub struct Node {
		#[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
		pub dependencies: BTreeMap<tg::Dependency, Entry>,
	}

	#[derive(
		Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
	)]
	pub struct Entry {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub package: Option<tg::directory::Id>,
		#[serde(with = "either::serde_untagged")]
		pub lock: Either<usize, Id>,
	}
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Lock, bytes))
	}
}

impl Lock {
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

	pub async fn object(&self, handle: &impl tg::Handle) -> tg::Result<Arc<Object>> {
		self.load(handle).await
	}

	pub async fn load(&self, handle: &impl tg::Handle) -> tg::Result<Arc<Object>> {
		self.try_load(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self, handle: &impl tg::Handle) -> tg::Result<Option<Arc<Object>>> {
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
		let root = object.root;
		let nodes = object
			.nodes
			.iter()
			.map(|node| node.data(handle, transaction))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { root, nodes })
	}
}

impl Lock {
	pub async fn dependencies(&self, handle: &impl tg::Handle) -> tg::Result<Vec<tg::Dependency>> {
		let object = self.object(handle).await?;
		let dependencies = object.nodes[object.root]
			.dependencies
			.keys()
			.cloned()
			.collect();
		Ok(dependencies)
	}

	pub async fn get(
		&self,
		handle: &impl tg::Handle,
		dependency: &tg::Dependency,
	) -> tg::Result<(Option<tg::Directory>, Lock)> {
		let object = self.object(handle).await?;
		let root = &object.nodes[object.root];
		let Entry { package, lock } = root
			.dependencies
			.get(dependency)
			.ok_or_else(|| tg::error!(%dependency, "failed to lookup dependency in lock"))?;
		let package = package.clone();

		// Short circuit if the lock is referred to by id.
		let index = match lock {
			Either::Left(index) => *index,
			Either::Right(lock) => return Ok((package, lock.clone())),
		};

		// Recursively construct a new lock.
		let mut nodes = vec![];
		let root = Self::get_inner(&mut nodes, &object, index);
		let lock = Lock::with_object(Object { root, nodes });

		Ok((package, lock))
	}

	fn get_inner(nodes: &mut Vec<Node>, object: &Object, index: usize) -> usize {
		let dependencies = object.nodes[index]
			.dependencies
			.iter()
			.map(|(dependency, lock)| {
				let Entry { package, lock } = lock;
				let package = package.clone();
				let lock = lock
					.as_ref()
					.map_left(|index| Self::get_inner(nodes, object, *index))
					.map_right(Lock::clone);
				let entry = Entry { package, lock };
				(dependency.clone(), entry)
			})
			.collect();
		let node = Node { dependencies };
		let index = nodes.len();
		nodes.push(node);
		index
	}
}

impl Lock {
	/// Read a lockfile.
	pub async fn read(path: impl AsRef<Path>) -> tg::Result<Self> {
		Self::try_read(path)
			.await?
			.ok_or_else(|| tg::error!("expected a lockfile to exist"))
	}

	/// Attempt to read a lockfile.
	pub async fn try_read(path: impl AsRef<Path>) -> tg::Result<Option<Self>> {
		let path = path.as_ref().join(tg::package::LOCKFILE_FILE_NAME);
		let bytes = match tokio::fs::read(&path).await {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(error) => {
				let path = path.display();
				return Err(tg::error!(source = error, %path, "failed to read the lockfile"));
			},
		};
		let data: Data = serde_json::from_slice(&bytes).map_err(|error| {
			let path = path.display();
			tg::error!(source = error, %path, "failed to deserialize the lockfile")
		})?;
		let object: Object = data.try_into()?;
		let lock = Self::with_object(object);

		Ok(Some(lock))
	}

	pub async fn write(&self, handle: &impl tg::Handle, path: tg::Path) -> tg::Result<()> {
		let path = path.join(tg::package::LOCKFILE_FILE_NAME);
		let data = self.data(handle, None).await?;
		let bytes = serde_json::to_vec_pretty(&data)
			.map_err(|source| tg::error!(!source, "failed to serialize the lock"))?;
		tokio::fs::write(&path, &bytes)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to write the lock file"))?;
		Ok(())
	}
}

impl Lock {
	pub async fn normalize(&self, handle: &impl tg::Handle) -> tg::Result<Self> {
		let mut visited = BTreeMap::new();
		let object = self.object(handle).await?;
		Self::normalize_inner(&object.nodes, object.root, &mut visited)
	}

	fn normalize_inner(
		nodes: &[Node],
		index: usize,
		visited: &mut BTreeMap<usize, Option<Lock>>,
	) -> tg::Result<Lock> {
		match visited.get(&index) {
			Some(None) => return Err(tg::error!("the lock contains a cycle")),
			Some(Some(lock)) => return Ok(lock.clone()),
			None => (),
		};
		visited.insert(index, None);
		let node = &nodes[index];
		let dependencies = node
			.dependencies
			.iter()
			.map(|(dependency, entry)| {
				let Some(index) = entry.lock.as_ref().left().copied() else {
					return Ok((dependency.clone(), entry.clone()));
				};
				let lock = Self::normalize_inner(nodes, index, visited)?;
				let entry = Entry {
					package: entry.package.clone(),
					lock: Either::Right(lock),
				};
				Ok::<_, tg::Error>((dependency.clone(), entry))
			})
			.try_collect()?;
		let node = Node { dependencies };
		let object = Object {
			root: 0,
			nodes: vec![node],
		};
		let lock = Lock::with_object(object);
		visited.insert(index, Some(lock.clone()));
		Ok(lock)
	}
}

impl Node {
	pub async fn data<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<data::Node>
	where
		H: tg::Handle,
	{
		let dependencies = self
			.dependencies
			.iter()
			.map(|(dependency, entry)| async move {
				Ok::<_, tg::Error>((dependency.clone(), entry.data(handle, transaction).await?))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(data::Node { dependencies })
	}
}

impl Entry {
	pub async fn data<H>(
		&self,
		handle: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<data::Entry>
	where
		H: tg::Handle,
	{
		let package = match &self.package {
			Some(package) => Some(package.id(handle, transaction).await?),
			None => None,
		};
		let lock = match &self.lock {
			Either::Left(index) => Either::Left(*index),
			Either::Right(lock) => Either::Right(lock.id(handle, transaction).await?),
		};
		Ok(data::Entry { package, lock })
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
		let mut children = Vec::new();
		for node in &self.nodes {
			for entry in node.dependencies.values() {
				if let Some(package) = &entry.package {
					children.push(package.clone().into());
				}
				if let Either::Right(id) = &entry.lock {
					children.push(id.clone().into());
				}
			}
		}
		children
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(value: Data) -> std::result::Result<Self, Self::Error> {
		let root = value.root;
		let nodes = value
			.nodes
			.into_iter()
			.map(TryInto::try_into)
			.try_collect()?;
		Ok(Self { root, nodes })
	}
}

impl TryFrom<data::Node> for Node {
	type Error = tg::Error;

	fn try_from(value: data::Node) -> std::result::Result<Self, Self::Error> {
		let dependencies = value
			.dependencies
			.into_iter()
			.map(|(dependency, entry)| Ok::<_, tg::Error>((dependency, entry.try_into()?)))
			.try_collect()?;
		Ok(Self { dependencies })
	}
}

impl TryFrom<data::Entry> for Entry {
	type Error = tg::Error;

	fn try_from(value: data::Entry) -> std::result::Result<Self, Self::Error> {
		let package = value.package.map(tg::Directory::with_id);
		let lock = match value.lock {
			Either::Left(index) => Either::Left(index),
			Either::Right(id) => Either::Right(Lock::with_id(id)),
		};
		Ok(Self { package, lock })
	}
}

impl Default for Lock {
	fn default() -> Self {
		Self::with_object(Object::default())
	}
}

impl Default for Object {
	fn default() -> Self {
		Self {
			root: 0,
			nodes: vec![Node::default()],
		}
	}
}

impl std::fmt::Display for Lock {
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
		if value.kind() != tg::id::Kind::Lock {
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
