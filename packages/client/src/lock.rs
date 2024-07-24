use crate as tg;
use bytes::Bytes;
use either::Either;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use itertools::Itertools as _;
use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

pub use self::data::Data;

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
pub struct Lock {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, Default)]
pub struct Node {
	pub dependencies: Option<BTreeMap<tg::Reference, Either<usize, tg::Object>>>,
	pub object: Option<tg::Object>,
}

pub mod data {
	use crate::{self as tg, util::serde::EitherUntagged};
	use either::Either;
	use serde_with::{serde_as, FromInto};
	use std::collections::BTreeMap;

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		pub nodes: Vec<Node>,
	}

	#[serde_as]
	#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
	pub struct Node {
		#[serde_as(as = "Option<BTreeMap<_, FromInto<EitherUntagged<usize, tg::object::Id>>>>")]
		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub dependencies: Option<BTreeMap<tg::Reference, Either<usize, tg::object::Id>>>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub object: Option<tg::object::Id>,
	}
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Lock, bytes))
	}
}

impl Lock {
	pub fn from_dependencies(dependencies: &BTreeMap<tg::Reference, tg::Object>) -> Self {
		let dependencies = dependencies
			.iter()
			.map(|(reference, object)| (reference.clone(), Either::Right(object.clone())))
			.collect();
		let node = Node {
			object: None,
			dependencies: Some(dependencies),
		};
		Lock::with_object(Object { nodes: vec![node] })
	}

	pub async fn try_inline(
		&self,
		handle: &impl tg::Handle,
		node: usize,
	) -> tg::Result<Option<BTreeMap<tg::Reference, tg::Object>>> {
		let object = self.object(handle).await?;
		let node = &object.nodes[node];
		let dependencies = node
			.dependencies
			.iter()
			.flatten()
			.map(|(reference, either)| {
				either
					.as_ref()
					.right()
					.map(|object| (reference.clone(), object.clone()))
					.ok_or_else(|| tg::error!("lock cannot be inlined"))
			})
			.try_collect()
			.ok();
		Ok(dependencies)
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
		let nodes = object
			.nodes
			.iter()
			.map(|node| node.data(handle))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		Ok(Data { nodes })
	}
}

impl Node {
	pub async fn data<H>(&self, handle: &H) -> tg::Result<data::Node>
	where
		H: tg::Handle,
	{
		let dependencies = if let Some(dependencies) = &self.dependencies {
			Some(
				dependencies
					.iter()
					.map(|(dependency, either)| async move {
						let dependency = dependency.clone();
						let option = match either {
							Either::Left(index) => Either::Left(*index),
							Either::Right(object) => Either::Right(object.id(handle).await?),
						};
						Ok::<_, tg::Error>((dependency, option))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?,
			)
		} else {
			None
		};
		let object = if let Some(object) = &self.object {
			Some(object.id(handle).await?)
		} else {
			None
		};
		Ok(data::Node {
			dependencies,
			object,
		})
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
		let mut children = BTreeSet::new();
		for node in &self.nodes {
			if let Some(object) = &node.object {
				children.insert(object.clone());
			}
			if let Some(dependencies) = &node.dependencies {
				for either in dependencies.values() {
					if let Either::Right(object) = either {
						children.insert(object.clone());
					}
				}
			}
		}
		children
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(value: Data) -> std::result::Result<Self, Self::Error> {
		let nodes = value
			.nodes
			.into_iter()
			.map(TryInto::try_into)
			.try_collect()?;
		Ok(Self { nodes })
	}
}

impl TryFrom<data::Node> for Node {
	type Error = tg::Error;

	fn try_from(value: data::Node) -> std::result::Result<Self, Self::Error> {
		let dependencies = if let Some(dependencies) = value.dependencies {
			Some(
				dependencies
					.into_iter()
					.map(|(dependency, either)| {
						let either = either.map_right(tg::Object::with_id);
						Ok::<_, tg::Error>((dependency, either))
					})
					.try_collect()?,
			)
		} else {
			None
		};
		let object = value.object.map(tg::Object::with_id);
		Ok(Self {
			dependencies,
			object,
		})
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
			nodes: vec![Node::default()],
		}
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
