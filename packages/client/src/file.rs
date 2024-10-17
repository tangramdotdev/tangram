use crate::{self as tg, util::serde::is_false};
use bytes::Bytes;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use itertools::Itertools as _;
use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};
use tangram_either::Either;

pub use self::builder::Builder;

pub mod builder;

/// The extended attribute name used to store file data.
pub const XATTR_DATA_NAME: &str = "user.tangram.data";
pub const XATTR_METADATA_NAME: &str = "user.tangram.metadata";

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
pub struct File {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub enum Object {
	Normal {
		contents: tg::Blob,
		dependencies: BTreeMap<tg::Reference, tg::Referent<tg::Object>>,
		executable: bool,
	},
	Graph {
		graph: tg::Graph,
		node: usize,
	},
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Data {
	Normal {
		contents: tg::blob::Id,

		#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
		dependencies: BTreeMap<tg::Reference, tg::Referent<tg::object::Id>>,

		#[serde(default, skip_serializing_if = "is_false")]
		executable: bool,
	},

	Graph {
		graph: tg::graph::Id,
		node: usize,
	},
}

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
		match object.as_ref() {
			Object::Normal {
				contents,
				dependencies,
				executable,
			} => {
				let contents = contents.id(handle).await?.clone();
				let dependencies = dependencies
					.iter()
					.map(|(reference, referent)| async move {
						let object = referent.item.id(handle).await?;
						let dependency = tg::Referent {
							item: object,
							subpath: None,
							tag: referent.tag.clone(),
						};
						Ok::<_, tg::Error>((reference.clone(), dependency))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
				let executable = *executable;
				Ok(Data::Normal {
					contents,
					dependencies,
					executable,
				})
			},
			Object::Graph { graph, node } => {
				let graph = graph.id(handle).await?;
				let node = *node;
				Ok(Data::Graph { graph, node })
			},
		}
	}
}

impl File {
	#[must_use]
	pub fn builder(contents: impl Into<tg::Blob>) -> Builder {
		Builder::new(contents)
	}

	#[must_use]
	pub fn with_contents(contents: impl Into<tg::Blob>) -> Self {
		Self::builder(contents).build()
	}

	#[must_use]
	pub fn with_graph_and_node(graph: tg::Graph, node: usize) -> Self {
		Self::with_object(Object::Graph { graph, node })
	}

	pub async fn contents<H>(&self, handle: &H) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Normal { contents, .. } => Ok(contents.clone()),
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let contents = file.contents.clone();
				Ok(contents)
			},
		}
	}

	pub async fn dependencies<H>(
		&self,
		handle: &H,
	) -> tg::Result<BTreeMap<tg::Reference, tg::Referent<tg::Object>>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let entries = match object.as_ref() {
			Object::Normal { dependencies, .. } => dependencies.clone(),
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				file.dependencies
					.iter()
					.map(|(reference, referent)| {
						let item = match &referent.item {
							Either::Left(index) => {
								let node = object
									.nodes
									.get(*index)
									.ok_or_else(|| tg::error!("invalid index"))?;
								match node {
									tg::graph::Node::Directory(_) => {
										tg::Directory::with_graph_and_node(graph.clone(), *index)
											.into()
									},
									tg::graph::Node::File(_) => {
										tg::File::with_graph_and_node(graph.clone(), *index).into()
									},
									tg::graph::Node::Symlink(_) => {
										tg::Symlink::with_graph_and_node(graph.clone(), *index)
											.into()
									},
								}
							},
							Either::Right(object) => object.clone(),
						};
						let referent = tg::Referent {
							item,
							subpath: None,
							tag: referent.tag.clone(),
						};
						Ok::<_, tg::Error>((reference.clone(), referent))
					})
					.try_collect()?
			},
		};
		Ok(entries)
	}

	pub async fn get_dependency<H>(
		&self,
		handle: &H,
		reference: &tg::Reference,
	) -> tg::Result<tg::Object>
	where
		H: tg::Handle,
	{
		self.try_get_dependency(handle, reference)
			.await?
			.ok_or_else(|| tg::error!("expected the dependency to exist"))
	}

	pub async fn try_get_dependency<H>(
		&self,
		handle: &H,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let object = match object.as_ref() {
			Object::Normal { dependencies, .. } => dependencies
				.get(reference)
				.map(|dependency| dependency.item.clone()),
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let Some(referent) = file.dependencies.get(reference) else {
					return Ok(None);
				};
				match &referent.item {
					Either::Left(index) => match object.nodes.get(*index) {
						Some(tg::graph::Node::Directory(_)) => {
							Some(tg::Directory::with_graph_and_node(graph.clone(), *index).into())
						},
						Some(tg::graph::Node::File(_)) => {
							Some(tg::File::with_graph_and_node(graph.clone(), *index).into())
						},
						Some(tg::graph::Node::Symlink(_)) => {
							Some(tg::Symlink::with_graph_and_node(graph.clone(), *index).into())
						},
						None => return Err(tg::error!("invalid index")),
					},
					Either::Right(object) => Some(object.clone()),
				}
			},
		};
		Ok(object)
	}

	pub async fn executable<H>(&self, handle: &H) -> tg::Result<bool>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Normal { executable, .. } => Ok(*executable),
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				Ok(file.executable)
			},
		}
	}

	pub async fn reader<H>(&self, handle: &H) -> tg::Result<tg::blob::Reader<H>>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.reader(handle).await
	}

	pub async fn size<H>(&self, handle: &H) -> tg::Result<u64>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.size(handle).await
	}

	pub async fn bytes<H>(&self, handle: &H) -> tg::Result<Vec<u8>>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.bytes(handle).await
	}

	pub async fn text<H>(&self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.text(handle).await
	}
}

impl Data {
	pub fn id(&self) -> tg::Result<Id> {
		Ok(Id::new(&self.serialize()?))
	}

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
		match self {
			Self::Normal {
				contents,
				dependencies,
				..
			} => {
				let contents = contents.clone().into();
				let dependencies = dependencies
					.values()
					.map(|dependency| dependency.item.clone());
				std::iter::once(contents).chain(dependencies).collect()
			},
			Self::Graph { graph, .. } => [graph.clone()].into_iter().map_into().collect(),
		}
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		match data {
			Data::Normal {
				contents,
				dependencies,
				executable,
			} => {
				let contents = tg::Blob::with_id(contents);
				let dependencies = dependencies
					.into_iter()
					.map(|(reference, referent)| {
						let referent = tg::Referent {
							item: tg::Object::with_id(referent.item),
							subpath: None,
							tag: referent.tag.clone(),
						};
						(reference, referent)
					})
					.collect();
				Ok(Self::Normal {
					contents,
					dependencies,
					executable,
				})
			},
			Data::Graph { graph, node } => {
				let graph = tg::Graph::with_id(graph);
				Ok(Self::Graph { graph, node })
			},
		}
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

impl From<tg::Blob> for File {
	fn from(value: tg::Blob) -> Self {
		Self::with_contents(value)
	}
}

impl From<String> for File {
	fn from(value: String) -> Self {
		Self::with_contents(value)
	}
}

impl From<&str> for File {
	fn from(value: &str) -> Self {
		Self::with_contents(value)
	}
}
