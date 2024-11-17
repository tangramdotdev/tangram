use super::{Data, Id, Object};
use crate as tg;
use std::{path::PathBuf, sync::Arc};
use tangram_either::Either;

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
			Object::Graph { graph, node } => {
				let graph = graph.id(handle).await?;
				let node = *node;
				Ok(Data::Graph { graph, node })
			},
			Object::Normal { artifact, subpath } => {
				let artifact = if let Some(artifact) = &artifact {
					Some(artifact.id(handle).await?)
				} else {
					None
				};
				let subpath = subpath.clone();
				Ok(Data::Normal { artifact, subpath })
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
			Object::Normal { artifact, .. } => Ok(artifact.clone()),
		}
	}

	pub async fn subpath<H>(&self, handle: &H) -> tg::Result<Option<PathBuf>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
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
			Object::Normal { subpath, .. } => Ok(subpath.clone()),
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
		let subpath = self.subpath(handle).await?.clone();
		match (artifact, subpath) {
			(None, Some(_)) => Err(tg::error!("cannot resolve a symlink with no artifact")),
			(Some(artifact), None) => Ok(Some(artifact)),
			(Some(tg::Artifact::Directory(directory)), Some(subpath)) => {
				directory.try_get(handle, subpath).await
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
	($artifact:expr, $subpath:expr) => {
		$crate::Symlink::with_artifact_and_subpath($artifact.into(), $subpath.into())
	};
}
