use super::{Data, Id, Object};
use crate as tg;
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug)]
pub struct Symlink {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

impl Symlink {
	#[must_use]
	pub fn with_state(state: State) -> Self {
		let state = Arc::new(std::sync::RwLock::new(state));
		Self { state }
	}

	#[must_use]
	pub fn state(&self) -> &Arc<std::sync::RwLock<State>> {
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

	#[must_use]
	pub fn id(&self) -> Id {
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return id;
		}
		let object = self.state.read().unwrap().object.clone().unwrap();
		let data = object.to_data();
		let bytes = data.serialize().unwrap();
		let id = Id::new(&bytes);
		self.state.write().unwrap().id.replace(id.clone());
		id
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
		let data = Data::deserialize(output.bytes)
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
		tg::Value::from(self.clone()).store(handle).await?;
		Ok(self.id())
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
		Ok(self.object(handle).await?.to_data())
	}
}

impl Symlink {
	#[must_use]
	pub fn with_graph_and_node(graph: tg::Graph, node: usize) -> Self {
		Self::with_object(Object::Graph(tg::symlink::object::Graph { graph, node }))
	}

	#[must_use]
	pub fn with_artifact_and_path(artifact: tg::Artifact, path: PathBuf) -> Self {
		// TODO: flatten graph artifacts.
		Self::with_object(Object::Node(tg::symlink::object::Node {
			artifact: Some(tg::graph::object::Edge::Object(artifact)),
			path: Some(path),
		}))
	}

	#[must_use]
	pub fn with_artifact(artifact: tg::Artifact) -> Self {
		// TODO: flatten graph artifacts.
		Self::with_object(Object::Node(tg::symlink::object::Node {
			artifact: Some(tg::graph::object::Edge::Object(artifact)),
			path: None,
		}))
	}

	#[must_use]
	pub fn with_path(path: PathBuf) -> Self {
		Self::with_object(Object::Node(tg::symlink::object::Node {
			artifact: None,
			path: Some(path),
		}))
	}

	pub async fn artifact<H>(&self, handle: &H) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Graph(object) => {
				let graph = &object.graph;
				let node = object.node;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let symlink = node
					.try_unwrap_symlink_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				let Some(artifact) = &symlink.artifact else {
					return Ok(None);
				};
				let artifact = match artifact {
					tg::graph::object::Edge::Graph(edge) => {
						let (graph, object) = if let Some(graph) = &edge.graph {
							let graph = graph.clone();
							let object = graph.object(handle).await?;
							(graph, object)
						} else {
							(graph.clone(), object.clone())
						};
						let kind = object
							.nodes
							.get(edge.node)
							.ok_or_else(|| tg::error!("invalid index"))?
							.kind();
						match kind {
							tg::artifact::Kind::Directory => {
								tg::Directory::with_graph_and_node(graph.clone(), edge.node).into()
							},
							tg::artifact::Kind::File => {
								tg::File::with_graph_and_node(graph.clone(), edge.node).into()
							},
							tg::artifact::Kind::Symlink => {
								tg::Symlink::with_graph_and_node(graph.clone(), edge.node).into()
							},
						}
					},
					tg::graph::object::Edge::Object(edge) => {
						edge.clone()
					},
				};
				Ok(Some(artifact))
			},
			Object::Node(node) => {
				let Some(artifact) = &node.artifact else {
					return Ok(None);
				};
				let artifact = match artifact.clone() {
					tg::graph::object::Edge::Graph(edge) => {
						let graph = edge.graph.ok_or_else(|| tg::error!("missing graph"))?;
						let object = graph.object(handle).await?;
						let kind = object
							.nodes
							.get(edge.node)
							.ok_or_else(|| tg::error!("invalid index"))?
							.kind();
						match kind {
							tg::artifact::Kind::Directory => {
								tg::Directory::with_graph_and_node(graph.clone(), edge.node).into()
							},
							tg::artifact::Kind::File => {
								tg::File::with_graph_and_node(graph.clone(), edge.node).into()
							},
							tg::artifact::Kind::Symlink => {
								tg::Symlink::with_graph_and_node(graph.clone(), edge.node).into()
							},
						}
					},
					tg::graph::object::Edge::Object(edge) => edge.clone()
				};
				Ok(Some(artifact))
			}
		}
	}

	pub async fn path<H>(&self, handle: &H) -> tg::Result<Option<PathBuf>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Graph(object) => {
				let graph = &object.graph;
				let node = object.node;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let symlink = node
					.try_unwrap_symlink_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				Ok(symlink.path.clone())
			},
			Object::Node(node) => Ok(node.path.clone()),
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
		if let Some(tg::Artifact::Symlink(symlink)) = artifact {
			artifact = Box::pin(symlink.try_resolve(handle)).await?;
		}
		let path = self.path(handle).await?.clone();
		match (artifact, path) {
			(None, Some(_)) => Err(tg::error!("cannot resolve a symlink with no artifact")),
			(Some(artifact), None) => Ok(Some(artifact)),
			(Some(tg::Artifact::Directory(directory)), Some(path)) => {
				directory.try_get(handle, path).await
			},
			_ => Err(tg::error!("invalid symlink")),
		}
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
	(artifact = $artifact:expr, path = $path:expr) => {
		$crate::Symlink::with_artifact_and_path($artifact.into(), $path.into())
	};
	(artifact = $artifact:expr) => {
		$crate::Symlink::with_artifact($artifact.into())
	};
	($path:expr) => {
		$crate::Symlink::with_path($path.into())
	};
}
