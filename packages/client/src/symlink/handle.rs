use {
	super::{Data, Id, Object},
	crate::prelude::*,
	std::{path::PathBuf, sync::Arc},
};

#[derive(Clone, Debug)]
pub struct Symlink {
	state: tg::object::State,
}

impl Symlink {
	#[must_use]
	pub fn with_state(state: tg::object::State) -> Self {
		Self { state }
	}

	#[must_use]
	pub fn state(&self) -> &tg::object::State {
		&self.state
	}

	#[must_use]
	pub fn with_id(id: Id) -> Self {
		Self::with_state(tg::object::State::with_id(id))
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<Object>>) -> Self {
		Self::with_state(tg::object::State::with_object(object.into()))
	}

	#[must_use]
	pub fn id(&self) -> Id {
		self.state.id().try_into().unwrap()
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
		self.try_load_with_arg(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load_with_arg<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		let object = self.state.try_load_with_arg(handle, arg).await?;
		let Some(object) = object else {
			return Ok(None);
		};
		let object = object.unwrap_symlink_ref().clone();
		Ok(Some(object))
	}

	pub fn unload(&self) {
		self.state.unload();
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
		self.state.children(handle).await
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
	pub fn with_reference(reference: tg::graph::Reference) -> Self {
		Self::with_object(Object::Reference(reference))
	}

	#[must_use]
	pub fn with_edge(edge: tg::graph::Edge<Self>) -> Self {
		match edge {
			tg::graph::Edge::Reference(reference) => Self::with_reference(reference),
			tg::graph::Edge::Object(symlink) => symlink,
		}
	}

	#[must_use]
	pub fn with_artifact_and_path(artifact: tg::Artifact, path: PathBuf) -> Self {
		Self::with_object(Object::Node(tg::symlink::object::Node {
			artifact: Some(tg::graph::Edge::Object(artifact)),
			path: Some(path),
		}))
	}

	#[must_use]
	pub fn with_artifact(artifact: tg::Artifact) -> Self {
		Self::with_object(Object::Node(tg::symlink::object::Node {
			artifact: Some(tg::graph::Edge::Object(artifact)),
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
			Object::Reference(object) => {
				let graph = object.graph.as_ref().unwrap();
				let index = object.index;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let symlink = node
					.try_unwrap_symlink_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				let Some(artifact) = &symlink.artifact else {
					return Ok(None);
				};
				let artifact = match artifact {
					tg::graph::Edge::Reference(reference) => {
						let graph = reference.graph.clone().unwrap_or_else(|| graph.clone());
						tg::Artifact::with_reference(tg::graph::Reference {
							graph: Some(graph),
							index: reference.index,
							kind: reference.kind,
						})
					},
					tg::graph::Edge::Object(object) => object.clone(),
				};
				Ok(Some(artifact))
			},
			Object::Node(node) => {
				let Some(artifact) = &node.artifact else {
					return Ok(None);
				};
				let artifact = match artifact.clone() {
					tg::graph::Edge::Reference(reference) => {
						let graph = reference.graph.ok_or_else(|| tg::error!("missing graph"))?;
						tg::Artifact::with_reference(tg::graph::Reference {
							graph: Some(graph),
							index: reference.index,
							kind: reference.kind,
						})
					},
					tg::graph::Edge::Object(object) => object.clone(),
				};
				Ok(Some(artifact))
			},
		}
	}

	pub async fn path<H>(&self, handle: &H) -> tg::Result<Option<PathBuf>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Reference(object) => {
				let graph = object.graph.as_ref().unwrap();
				let index = object.index;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(index)
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
