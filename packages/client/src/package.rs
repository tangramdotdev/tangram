use crate::{self as tg, util::arc::Ext as _};
use bytes::Bytes;
use either::Either;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	FutureExt as _, TryStreamExt as _,
};
use itertools::Itertools as _;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::Path,
	sync::Arc,
};

pub use self::data::Data;

pub mod check;
pub mod checkin;
pub mod doc;
pub mod format;
pub mod module;

/// The possible file names of the default file.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

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
pub struct Package {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub nodes: Vec<Node>,
	pub root: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Node {
	pub dependencies: BTreeMap<tg::Reference, Either<usize, tg::Object>>,
	pub metadata: BTreeMap<String, tg::Value>,
	pub object: Option<tg::Object>,
}

pub mod data {
	use crate::{
		self as tg,
		util::serde::{is_zero, EitherUntagged},
	};
	use either::Either;
	use serde_with::{serde_as, FromInto};
	use std::collections::BTreeMap;

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		pub nodes: Vec<Node>,

		#[serde(default, skip_serializing_if = "is_zero")]
		pub root: usize,
	}

	#[serde_as]
	#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
	pub struct Node {
		#[serde_as(as = "BTreeMap<_, FromInto<EitherUntagged<usize, tg::object::Id>>>")]
		pub dependencies: BTreeMap<tg::Reference, Either<usize, tg::object::Id>>,

		#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
		pub metadata: BTreeMap<String, tg::value::Data>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		pub object: Option<tg::object::Id>,
	}
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Package, bytes))
	}
}

impl Package {
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
		let root = object.root;
		Ok(Data { nodes, root })
	}
}

impl Package {
	pub async fn dependencies<H>(&self, handle: &H) -> tg::Result<Vec<tg::Reference>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let dependencies = object.nodes[object.root]
			.dependencies
			.keys()
			.cloned()
			.collect();
		Ok(dependencies)
	}

	pub async fn get_dependency<H>(
		&self,
		handle: &H,
		dependency: &tg::Reference,
	) -> tg::Result<tg::Object>
	where
		H: tg::Handle,
	{
		self.try_get_dependency(handle, dependency)
			.map(|result| {
				result.and_then(|option| {
					option.ok_or_else(|| tg::error!(%dependency, "failed to find the dependency"))
				})
			})
			.await
	}

	pub async fn try_get_dependency<H>(
		&self,
		handle: &H,
		dependency: &tg::Reference,
	) -> tg::Result<Option<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let root = object.nodes.get(object.root).unwrap();
		let Some(either) = root.dependencies.get(dependency) else {
			return Ok(None);
		};
		let index = match either {
			Either::Left(index) => *index,
			Either::Right(object) => return Ok(Some(object.clone())),
		};
		let mut nodes = vec![];
		let root = Self::try_get_dependency_inner(&mut nodes, &object, index);
		let package = Package::with_object(Object { nodes, root });
		let object = package.into();
		Ok(Some(object))
	}

	fn try_get_dependency_inner(nodes: &mut Vec<Node>, package: &Object, index: usize) -> usize {
		let node = &package.nodes[index];
		let dependencies = node
			.dependencies
			.iter()
			.map(|(dependency, either)| {
				let either = either
					.as_ref()
					.map_left(|index| Self::try_get_dependency_inner(nodes, package, *index))
					.map_right(tg::Object::clone);
				(dependency.clone(), either)
			})
			.collect();
		let metadata = node.metadata.clone();
		let object = node.object.clone();
		let node = Node {
			dependencies,
			metadata,
			object,
		};
		let index = nodes.len();
		nodes.push(node);
		index
	}

	pub async fn metadata<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl std::ops::Deref<Target = BTreeMap<String, tg::Value>>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object(handle)
			.await?
			.map(|object| &object.nodes.get(object.root).unwrap().metadata))
	}

	pub async fn normalize<H>(&self, handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let mut visited = BTreeMap::new();
		let object = self.object(handle).await?;
		Self::normalize_inner(&object.nodes, object.root, &mut visited)
	}

	fn normalize_inner(
		nodes: &[Node],
		index: usize,
		visited: &mut BTreeMap<usize, Option<Package>>,
	) -> tg::Result<Package> {
		match visited.get(&index) {
			Some(None) => return Err(tg::error!("the package contains a cycle")),
			Some(Some(package)) => return Ok(package.clone()),
			None => (),
		};
		visited.insert(index, None);
		let node = &nodes[index];
		let dependencies = node
			.dependencies
			.iter()
			.map(|(dependency, either)| {
				let dependency = dependency.clone();
				let either = match either {
					Either::Left(index) => {
						let package = Self::normalize_inner(nodes, *index, visited)?;
						let object = package.into();
						Either::Right(object)
					},
					Either::Right(object) => Either::Right(object.clone()),
				};
				Ok::<_, tg::Error>((dependency, either))
			})
			.try_collect()?;
		let metadata = node.metadata.clone();
		let object = node.object.clone();
		let node = Node {
			dependencies,
			metadata,
			object,
		};
		let object = Object {
			root: 0,
			nodes: vec![node],
		};
		let package = Package::with_object(object);
		visited.insert(index, Some(package.clone()));
		Ok(package)
	}
}

impl Node {
	pub async fn data<H>(&self, handle: &H) -> tg::Result<data::Node>
	where
		H: tg::Handle,
	{
		let dependencies = self
			.dependencies
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
			.await?;
		let metadata = self
			.metadata
			.iter()
			.map(|(key, value)| async move {
				Ok::<_, tg::Error>((key.clone(), value.data(handle).await?))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		let object = match &self.object {
			Some(object) => Some(object.id(handle).await?),
			None => None,
		};
		Ok(data::Node {
			dependencies,
			metadata,
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
			for either in node.dependencies.values() {
				if let Either::Right(object) = either {
					children.insert(object.clone());
				}
			}
		}
		children
	}
}

pub async fn get_root_module_path_for_path(path: &Path) -> tg::Result<tg::Path> {
	try_get_root_module_path_for_path(path).await?.ok_or_else(
		|| tg::error!(%path = path.display(), "failed to find the package's root module"),
	)
}

pub async fn try_get_root_module_path_for_path(path: &Path) -> tg::Result<Option<tg::Path>> {
	let mut root_module_path = None;
	for module_file_name in tg::package::ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(module_file_name))
			.await
			.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to get the metadata"),
			)? {
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(value: Data) -> std::result::Result<Self, Self::Error> {
		let nodes = value
			.nodes
			.into_iter()
			.map(TryInto::try_into)
			.try_collect()?;
		let root = value.root;
		Ok(Self { nodes, root })
	}
}

impl TryFrom<data::Node> for Node {
	type Error = tg::Error;

	fn try_from(value: data::Node) -> std::result::Result<Self, Self::Error> {
		let dependencies = value
			.dependencies
			.into_iter()
			.map(|(dependency, either)| {
				let either = either.map_right(tg::Object::with_id);
				Ok::<_, tg::Error>((dependency, either))
			})
			.try_collect()?;
		let metadata = value
			.metadata
			.into_iter()
			.map(|(key, value)| Ok::<_, tg::Error>((key, value.try_into()?)))
			.try_collect()?;
		let object = value.object.map(tg::Object::with_id);
		Ok(Self {
			dependencies,
			metadata,
			object,
		})
	}
}

impl Default for Package {
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

impl std::fmt::Display for Package {
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
		if value.kind() != tg::id::Kind::Package {
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

impl From<tg::Artifact> for tg::Package {
	fn from(value: tg::Artifact) -> Self {
		Self::with_object(Object {
			root: 0,
			nodes: vec![Node {
				object: Some(value.into()),
				..Default::default()
			}],
		})
	}
}
