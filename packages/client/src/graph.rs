use crate as tg;
use bytes::Bytes;
use futures::{
	stream::{FuturesOrdered, FuturesUnordered},
	TryStreamExt as _,
};
use itertools::Itertools as _;
use std::{collections::BTreeSet, sync::Arc};
use tangram_either::Either;

pub use self::{data::Data, node::Node};

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
pub struct Graph {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub struct Object {
	pub nodes: Vec<Node>,
}

pub mod node {
	use crate as tg;
	use std::collections::BTreeMap;
	use tangram_either::Either;

	#[derive(Clone, Debug, derive_more::TryUnwrap)]
	#[try_unwrap(ref)]
	pub enum Node {
		Directory(Directory),
		File(File),
		Symlink(Symlink),
	}

	#[derive(Clone, Debug)]
	pub enum Kind {
		Directory,
		File,
		Symlink,
	}

	#[derive(Clone, Debug)]
	pub struct Directory {
		pub entries: BTreeMap<String, Either<usize, tg::Artifact>>,
	}

	#[derive(Clone, Debug)]
	pub struct File {
		pub contents: tg::Blob,
		pub dependencies: Option<Either<DependenciesArray, DependenciesMap>>,
		pub executable: bool,
	}

	type DependenciesArray = Vec<Either<usize, tg::Object>>;

	type DependenciesMap = BTreeMap<tg::Reference, Either<usize, tg::Object>>;

	#[derive(Clone, Debug)]
	pub struct Symlink {
		pub artifact: Option<Either<usize, tg::Artifact>>,
		pub path: Option<tg::Path>,
	}
}

pub mod data {
	pub use self::node::Node;

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Data {
		pub nodes: Vec<Node>,
	}

	pub mod node {
		use crate::{self as tg, util::serde::is_false};
		use std::collections::BTreeMap;
		use tangram_either::Either;

		#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
		#[serde(tag = "kind", rename_all = "snake_case")]
		pub enum Node {
			Directory(Directory),
			File(File),
			Symlink(Symlink),
		}

		#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
		pub struct Directory {
			pub entries: BTreeMap<String, Either<usize, tg::artifact::Id>>,
		}

		#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
		pub struct File {
			pub contents: tg::blob::Id,

			#[serde(default, skip_serializing_if = "Option::is_none")]
			pub dependencies: Option<Either<DependenciesArray, DependenciesMap>>,

			#[serde(default, skip_serializing_if = "is_false")]
			pub executable: bool,
		}

		type DependenciesArray = Vec<Either<usize, tg::object::Id>>;

		type DependenciesMap = BTreeMap<tg::Reference, Either<usize, tg::object::Id>>;

		#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
		pub struct Symlink {
			#[serde(default, skip_serializing_if = "Option::is_none")]
			pub artifact: Option<Either<usize, tg::artifact::Id>>,

			#[serde(default, skip_serializing_if = "Option::is_none")]
			pub path: Option<tg::Path>,
		}

		impl Node {
			#[must_use]
			pub fn kind(&self) -> tg::artifact::Kind {
				match self {
					Self::Directory(_) => tg::artifact::Kind::Directory,
					Self::File(_) => tg::artifact::Kind::File,
					Self::Symlink(_) => tg::artifact::Kind::Symlink,
				}
			}
		}
	}
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Graph, bytes))
	}
}

impl Graph {
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
		match self {
			Self::Directory(tg::graph::node::Directory { entries }) => {
				let entries = entries
					.iter()
					.map(|(name, either)| async move {
						let artifact = match either {
							Either::Left(index) => Either::Left(*index),
							Either::Right(artifact) => Either::Right(artifact.id(handle).await?),
						};
						Ok::<_, tg::Error>((name.clone(), artifact))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
				Ok(data::Node::Directory(tg::graph::data::node::Directory {
					entries,
				}))
			},

			Self::File(tg::graph::node::File {
				contents,
				dependencies,
				executable,
			}) => {
				let contents = contents.id(handle).await?;
				let dependencies = if let Some(dependencies) = &dependencies {
					match dependencies {
						Either::Left(dependencies) => Some(
							dependencies
								.iter()
								.map(|either| async move {
									let either = match either {
										Either::Left(index) => Either::Left(*index),
										Either::Right(object) => {
											Either::Right(object.id(handle).await?)
										},
									};
									Ok::<_, tg::Error>(either)
								})
								.collect::<FuturesUnordered<_>>()
								.try_collect()
								.await
								.map(Either::Left)?,
						),
						Either::Right(dependencies) => Some(
							dependencies
								.iter()
								.map(|(reference, either)| async move {
									let reference = reference.clone();
									let either = match either {
										Either::Left(index) => Either::Left(*index),
										Either::Right(object) => {
											Either::Right(object.id(handle).await?)
										},
									};
									Ok::<_, tg::Error>((reference, either))
								})
								.collect::<FuturesUnordered<_>>()
								.try_collect()
								.await
								.map(Either::Right)?,
						),
					}
				} else {
					None
				};
				let executable = *executable;
				Ok(data::Node::File(tg::graph::data::node::File {
					contents,
					dependencies,
					executable,
				}))
			},

			Self::Symlink(tg::graph::node::Symlink { artifact, path }) => {
				let artifact = if let Some(artifact) = artifact {
					Some(match artifact {
						Either::Left(index) => Either::Left(*index),
						Either::Right(artifact) => Either::Right(artifact.id(handle).await?),
					})
				} else {
					None
				};
				let path = path.clone();
				Ok(data::Node::Symlink(tg::graph::data::node::Symlink {
					artifact,
					path,
				}))
			},
		}
	}
}

impl Node {
	#[must_use]
	pub fn kind(&self) -> tg::artifact::Kind {
		match self {
			Self::Directory(_) => tg::artifact::Kind::Directory,
			Self::File(_) => tg::artifact::Kind::File,
			Self::Symlink(_) => tg::artifact::Kind::Symlink,
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
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		let mut children = BTreeSet::new();
		for node in &self.nodes {
			match node {
				data::Node::Directory(tg::graph::data::node::Directory { entries }) => {
					for either in entries.values() {
						if let Either::Right(id) = either {
							children.insert(id.clone().into());
						}
					}
				},
				data::Node::File(tg::graph::data::node::File {
					contents,
					dependencies,
					..
				}) => {
					children.insert(contents.clone().into());
					if let Some(dependencies) = dependencies {
						match dependencies {
							Either::Left(dependencies) => {
								for either in dependencies {
									if let Either::Right(id) = either {
										children.insert(id.clone());
									}
								}
							},
							Either::Right(dependencies) => {
								for either in dependencies.values() {
									if let Either::Right(id) = either {
										children.insert(id.clone());
									}
								}
							},
						}
					}
				},
				data::Node::Symlink(tg::graph::data::node::Symlink { artifact, .. }) => {
					if let Some(Either::Right(id)) = artifact {
						children.insert(id.clone().into());
					}
				},
			}
		}
		children
	}

	pub fn id(&self) -> tg::Result<Id> {
		Ok(Id::new(&self.serialize()?))
	}

	pub fn id_of_node(&self, node: usize) -> tg::Result<tg::artifact::Id> {
		let data = self
			.nodes
			.get(node)
			.ok_or_else(|| tg::error!("index out of bounds"))?;
		match data {
			data::Node::Directory(_) => Ok(tg::directory::Data::Graph {
				graph: self.id()?,
				node,
			}
			.id()?
			.into()),
			data::Node::File(_) => Ok(tg::file::Data::Graph {
				graph: self.id()?,
				node,
			}
			.id()?
			.into()),
			data::Node::Symlink(_) => Ok(tg::symlink::Data::Graph {
				graph: self.id()?,
				node,
			}
			.id()?
			.into()),
		}
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
		match value {
			data::Node::Directory(tg::graph::data::node::Directory { entries }) => {
				let entries = entries
					.into_iter()
					.map(|(name, either)| (name, either.map_right(tg::Artifact::with_id)))
					.collect();
				let directory = tg::graph::node::Directory { entries };
				let node = Node::Directory(directory);
				Ok(node)
			},
			data::Node::File(tg::graph::data::node::File {
				contents,
				dependencies,
				executable,
			}) => {
				let contents = tg::Blob::with_id(contents);
				let dependencies = dependencies.map(|dependencies| match dependencies {
					Either::Left(dependencies) => Either::Left(
						dependencies
							.into_iter()
							.map(|either| match either {
								Either::Left(index) => Either::Left(index),
								Either::Right(object) => Either::Right(tg::Object::with_id(object)),
							})
							.collect(),
					),
					Either::Right(dependencies) => Either::Right(
						dependencies
							.into_iter()
							.map(|(reference, either)| {
								let either = match either {
									Either::Left(index) => Either::Left(index),
									Either::Right(object) => {
										Either::Right(tg::Object::with_id(object))
									},
								};
								(reference, either)
							})
							.collect(),
					),
				});
				let file = tg::graph::node::File {
					contents,
					dependencies,
					executable,
				};
				let node = Node::File(file);
				Ok(node)
			},
			data::Node::Symlink(tg::graph::data::node::Symlink { artifact, path }) => {
				let artifact = artifact.map(|either| either.map_right(tg::Artifact::with_id));
				let symlink = tg::graph::node::Symlink { artifact, path };
				let node = Node::Symlink(symlink);
				Ok(node)
			},
		}
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Graph {
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

impl std::fmt::Display for tg::graph::node::Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
		}
	}
}

impl std::str::FromStr for tg::graph::node::Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}
