pub use self::data::Data;
use crate::{error, id, object, Dependency, Directory, Error, Handle, Result};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::Display;
use either::Either;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt,
};
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

pub type State = object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub root: usize,
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, Default)]
pub struct Node {
	pub dependencies: BTreeMap<Dependency, Entry>,
}

#[derive(Clone, Debug)]
pub struct Entry {
	pub package: Option<Directory>,
	pub lock: Either<usize, Lock>,
}

pub mod data {
	use super::Id;
	use crate::{directory, Dependency};
	use either::Either;
	use serde_with::serde_as;
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
		#[serde_as(as = "BTreeMap<serde_with::DisplayFromStr, _>")]
		pub dependencies: BTreeMap<Dependency, Entry>,
	}

	#[derive(
		Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
	)]
	pub struct Entry {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub package: Option<directory::Id>,
		#[serde(with = "either::serde_untagged")]
		pub lock: Either<usize, Id>,
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
			.ok_or_else(|| error!("Failed to load the object."))
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
			.map_err(|error| error!(source = error, "Failed to deserialize the data."))?;
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
			.map_err(|error| error!(source = error, "Failed to put the object."))?;
		self.state.write().unwrap().id.replace(id);
		Ok(())
	}

	#[async_recursion]
	pub async fn data(&self, tg: &dyn Handle) -> Result<Data> {
		let object = self.object(tg).await?;
		let root = object.root;
		let nodes = object
			.nodes
			.iter()
			.map(|node| node.data(tg))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { root, nodes })
	}
}

impl Lock {
	pub async fn get(
		&self,
		tg: &dyn Handle,
		dependency: &Dependency,
	) -> Result<Option<(Directory, Lock)>> {
		let object = self.object(tg).await?;
		let root = &object.nodes[object.root];
		let Entry { package, lock } = root
			.dependencies
			.get(dependency)
			.ok_or_else(|| error!(%dependency, "Failed to lookup dependency in lock."))?;

		// Get the package if it exists.
		let package = package
			.as_ref()
			.ok_or_else(|| error!("Expected a package for dependency."))?;

		// Short circuit if the lock is referred to by id.
		let index = match lock {
			Either::Left(index) => *index,
			Either::Right(lock) => return Ok(Some((package.clone(), lock.clone()))),
		};

		// Recursively construct a new lock.
		let mut nodes = vec![];
		let root = Self::get_inner(&mut nodes, object, index);
		let lock = Lock::with_object(Object { root, nodes });
		Ok(Some((package.clone(), lock)))
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
	pub async fn normalize(&self, tg: &dyn Handle) -> Result<Self> {
		let mut visited = BTreeMap::new();
		let object = self.object(tg).await?;
		Self::normalize_inner(&object.nodes, object.root, &mut visited)
	}

	fn normalize_inner(
		nodes: &[Node],
		index: usize,
		visited: &mut BTreeMap<usize, Option<Lock>>,
	) -> Result<Lock> {
		match visited.get(&index) {
			Some(None) => return Err(error!("The lock contains a cycle.")),
			Some(Some(lock)) => return Ok(lock.clone()),
			None => (),
		};
		visited.insert(index, None);
		let node = &nodes[index];
		let dependencies = node
			.dependencies
			.iter()
			.filter_map(|(dependency, entry)| {
				let index = *entry.lock.as_ref().left()?;
				let lock = match Self::normalize_inner(nodes, index, visited) {
					Ok(lock) => lock,
					Err(e) => return Some(Err(e)),
				};
				let entry = Entry {
					package: entry.package.clone(),
					lock: Either::Right(lock),
				};
				Some(Ok((dependency.clone(), entry)))
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
	pub async fn data(&self, tg: &dyn Handle) -> Result<data::Node> {
		let dependencies = self
			.dependencies
			.iter()
			.map(|(dependency, entry)| async move {
				Ok::<_, Error>((dependency.clone(), entry.data(tg).await?))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		Ok(data::Node { dependencies })
	}
}

impl Entry {
	pub async fn data(&self, tg: &dyn Handle) -> Result<data::Entry> {
		let package = match &self.package {
			Some(package) => Some(package.id(tg).await?.clone()),
			None => None,
		};
		let lock = match &self.lock {
			Either::Left(index) => Either::Left(*index),
			Either::Right(lock) => Either::Right(lock.id(tg).await?.clone()),
		};
		Ok(data::Entry { package, lock })
	}
}

impl Data {
	pub fn serialize(&self) -> Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|error| error!(source = error, "Failed to serialize the data."))
	}

	pub fn deserialize(bytes: &Bytes) -> Result<Self> {
		serde_json::from_reader(bytes.as_ref())
			.map_err(|error| error!(source = error, "Failed to deserialize the data."))
	}

	#[must_use]
	pub fn children(&self) -> Vec<object::Id> {
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
	type Error = Error;

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
	type Error = Error;

	fn try_from(value: data::Node) -> std::result::Result<Self, Self::Error> {
		let dependencies = value
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
		let package = value.package.map(Directory::with_id);
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
			return Err(error!(%value, "Invalid kind."));
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
