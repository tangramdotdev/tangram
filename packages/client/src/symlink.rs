use crate as tg;
use bytes::Bytes;
use itertools::Itertools as _;
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
use tangram_either::Either;

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
pub struct Symlink {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

#[derive(Clone, Debug)]
pub enum Object {
	Normal {
		artifact: Option<tg::Artifact>,
		subpath: Option<PathBuf>,
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
		#[serde(default, skip_serializing_if = "Option::is_none")]
		artifact: Option<tg::artifact::Id>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		subpath: Option<PathBuf>,
	},

	Graph {
		graph: tg::graph::Id,
		node: usize,
	},
}

impl Id {
	pub fn new(bytes: &Bytes) -> Self {
		Self(crate::Id::new_blake3(tg::id::Kind::Symlink, bytes))
	}
}

impl Symlink {
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

	pub async fn children<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		let object = self.load(handle).await?;
		Ok(object.children())
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Normal { artifact, subpath } => {
				let artifact = if let Some(artifact) = &artifact {
					Some(artifact.id(handle).await?)
				} else {
					None
				};
				let subpath = subpath.clone();
				Ok(Data::Normal { artifact, subpath })
			},
			Object::Graph { graph, node } => {
				let graph = graph.id(handle).await?;
				let node = *node;
				Ok(Data::Graph { graph, node })
			},
		}
	}
}

impl Symlink {
	#[must_use]
	pub fn with_artifact_and_subpath(
		artifact: Option<tg::Artifact>,
		subpath: Option<PathBuf>,
	) -> Self {
		Self::with_object(Object::Normal { artifact, subpath })
	}

	#[must_use]
	pub fn with_graph_and_node(graph: tg::Graph, node: usize) -> Self {
		Self::with_object(Object::Graph { graph, node })
	}

	pub async fn artifact<H>(&self, handle: &H) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Normal { artifact, .. } => Ok(artifact.clone()),
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let symlink = node
					.try_unwrap_symlink_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				let artifact = if let Some(either) = &symlink.artifact {
					let artifact = match either {
						Either::Left(node) => {
							let kind = object
								.nodes
								.get(*node)
								.ok_or_else(|| tg::error!("invalid index"))?
								.kind();
							match kind {
								tg::artifact::Kind::Directory => {
									tg::Directory::with_graph_and_node(graph.clone(), *node).into()
								},
								tg::artifact::Kind::File => {
									tg::File::with_graph_and_node(graph.clone(), *node).into()
								},
								tg::artifact::Kind::Symlink => {
									tg::Symlink::with_graph_and_node(graph.clone(), *node).into()
								},
							}
						},
						Either::Right(artifact) => artifact.clone(),
					};
					Some(artifact)
				} else {
					None
				};
				Ok(artifact)
			},
		}
	}

	pub async fn subpath<H>(&self, handle: &H) -> tg::Result<Option<PathBuf>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Normal { subpath, .. } => Ok(subpath.clone()),
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let symlink = node
					.try_unwrap_symlink_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				Ok(symlink.subpath.clone())
			},
		}
	}

	pub async fn resolve<H>(&self, handle: &H) -> tg::Result<tg::Artifact>
	where
		H: tg::Handle,
	{
		self.try_resolve(handle)
			.await?
			.ok_or_else(|| tg::error!("broken symlink"))
	}

	pub async fn try_resolve<H>(&self, handle: &H) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		let mut artifact = self.artifact(handle).await?.clone();
		if let Some(tg::artifact::Artifact::Symlink(symlink)) = artifact {
			artifact = Box::pin(symlink.try_resolve(handle)).await?;
		}
		let path = self.subpath(handle).await?.clone();
		if artifact.is_some() && path.is_none() {
			return Ok(artifact);
		} else if artifact.is_none() && path.is_some() {
			return Err(tg::error!("cannot resolve with no artifact"));
		} else if artifact.is_some() && path.is_some() {
			if let Some(tg::artifact::Artifact::Directory(directory)) = artifact {
				return directory.try_get(handle, &path.unwrap_or_default()).await;
			}
			return Err(tg::error!("expected a directory"));
		}
		Err(tg::error!("invalid symlink"))
	}
}

impl Object {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Normal { artifact, .. } => artifact.clone().into_iter().map_into().collect(),
			Self::Graph { graph, .. } => [graph.clone()].into_iter().map_into().collect(),
		}
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
			Self::Normal { artifact, .. } => artifact.clone().into_iter().map_into().collect(),
			Self::Graph { graph, .. } => [graph.clone()].into_iter().map_into().collect(),
		}
	}
}

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		match data {
			Data::Normal { artifact, subpath } => {
				let artifact = artifact.map(tg::Artifact::with_id);
				Ok(Self::Normal { artifact, subpath })
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
		if value.kind() != tg::id::Kind::Symlink {
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

impl std::fmt::Display for Symlink {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.symlink(self)?;
		Ok(())
	}
}

#[macro_export]
macro_rules! symlink {
	($artifact:expr, $subpath:expr) => {
		$crate::Symlink::with_artifact_and_subpath($artifact.into(), $subpath.into())
	};
}
