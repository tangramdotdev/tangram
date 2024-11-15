use super::{Builder, Data, Id, Object};
use crate as tg;
use futures::{
	future::BoxFuture, stream::FuturesUnordered, Future, FutureExt, TryFutureExt, TryStreamExt as _,
};
use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_either::Either;

#[derive(Clone, Debug)]
pub struct Directory {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

impl Directory {
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
			Object::Normal { entries } => {
				#[allow(clippy::manual_async_fn)]
				fn future(
					handle: impl tg::Handle,
					artifact: tg::Artifact,
				) -> impl Future<Output = tg::Result<tg::artifact::Id>> + Send {
					async move { artifact.id(&handle).await }
				}
				let entries = entries
					.iter()
					.map(|(name, artifact)| {
						let artifact = artifact.clone();
						let handle = handle.clone();
						async move {
							let artifact = tokio::spawn(future(handle, artifact))
								.map(|result| result.unwrap())
								.await?;
							Ok::<_, tg::Error>((name.clone(), artifact))
						}
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
				Ok(Data::Normal { entries })
			},
		}
	}
}

impl Directory {
	#[must_use]
	pub fn with_entries(entries: BTreeMap<String, tg::Artifact>) -> Self {
		Self::with_object(Object::Normal { entries })
	}

	#[must_use]
	pub fn with_graph_and_node(graph: tg::Graph, node: usize) -> Self {
		Self::with_object(Object::Graph { graph, node })
	}

	pub async fn builder<H>(&self, handle: &H) -> tg::Result<Builder>
	where
		H: tg::Handle,
	{
		let entries = self.entries(handle).await?;
		let builder = Builder::with_entries(entries);
		Ok(builder)
	}

	pub async fn entries<H>(&self, handle: &H) -> tg::Result<BTreeMap<String, tg::Artifact>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let entries = match object.as_ref() {
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let directory = node
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				directory
					.entries
					.iter()
					.map(|(name, either)| {
						let artifact = match either {
							Either::Left(node) => {
								let kind = object
									.nodes
									.get(*node)
									.ok_or_else(|| tg::error!("invalid index"))?
									.kind();
								match kind {
									tg::artifact::Kind::Directory => {
										tg::Directory::with_graph_and_node(graph.clone(), *node)
											.into()
									},
									tg::artifact::Kind::File => {
										tg::File::with_graph_and_node(graph.clone(), *node).into()
									},
									tg::artifact::Kind::Symlink => {
										tg::Symlink::with_graph_and_node(graph.clone(), *node)
											.into()
									},
								}
							},
							Either::Right(artifact) => artifact.clone(),
						};
						Ok((name.clone(), artifact))
					})
					.collect::<tg::Result<_>>()?
			},
			Object::Normal { entries } => entries.clone(),
		};
		Ok(entries)
	}

	pub async fn try_get_entry<H>(&self, handle: &H, name: &str) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let artifact = match object.as_ref() {
			Object::Graph { graph, node } => {
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(*node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let directory = node
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				match directory.entries.get(name) {
					None => None,
					Some(Either::Left(node)) => {
						let kind = object
							.nodes
							.get(*node)
							.ok_or_else(|| tg::error!("invalid index"))?
							.kind();
						let artifact = match kind {
							tg::artifact::Kind::Directory => {
								tg::Directory::with_graph_and_node(graph.clone(), *node).into()
							},
							tg::artifact::Kind::File => {
								tg::File::with_graph_and_node(graph.clone(), *node).into()
							},
							tg::artifact::Kind::Symlink => {
								tg::Symlink::with_graph_and_node(graph.clone(), *node).into()
							},
						};
						Some(artifact)
					},
					Some(Either::Right(artifact)) => Some(artifact.clone()),
				}
			},
			Object::Normal { entries } => entries.get(name).cloned(),
		};
		Ok(artifact)
	}

	pub async fn get<H>(&self, handle: &H, path: impl AsRef<Path>) -> tg::Result<tg::Artifact>
	where
		H: tg::Handle,
	{
		let artifact = self
			.try_get(handle, path)
			.await?
			.ok_or_else(|| tg::error!("failed to get the artifact"))?;
		Ok(artifact)
	}

	pub async fn try_get<H>(
		&self,
		handle: &H,
		path: impl AsRef<Path>,
	) -> tg::Result<Option<tg::Artifact>>
	where
		H: tg::Handle,
	{
		let path = path.as_ref();
		if path.is_absolute() {
			return Err(tg::error!(%path = path.display(), "expected a relative path"));
		}

		let mut artifact: tg::Artifact = self.clone().into();

		// Track the current path.
		let mut current_path = PathBuf::new();

		// Handle each path component.
		for component in path.components() {
			// Skip current directory components.
			if matches!(component, std::path::Component::CurDir) {
				continue;
			}

			// The artifact must be a directory.
			let Some(directory) = artifact.try_unwrap_directory_ref().ok() else {
				return Ok(None);
			};

			// Update the current path.
			current_path.push(component);

			// Get the entry. If it doesn't exist, return `None`.
			let std::path::Component::Normal(name) = component else {
				return Err(
					tg::error!(%path = path.display(), "the path must contain only normal components"),
				);
			};

			let name = name
				.to_str()
				.ok_or_else(|| tg::error!("expected a utf-8 encoded path"))?;

			let Some(entry) = directory.try_get_entry(handle, name).await? else {
				return Ok(None);
			};

			// Get the artifact.
			artifact = entry;

			// If the artifact is a symlink, then resolve it.
			if let tg::Artifact::Symlink(symlink) = &artifact {
				match Box::pin(symlink.try_resolve(handle))
					.await
					.map_err(|source| tg::error!(!source, "failed to resolve the symlink"))?
				{
					Some(resolved) => artifact = resolved,
					None => return Ok(None),
				}
			}
		}

		Ok(Some(artifact))
	}
}

impl std::fmt::Display for Directory {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.directory(self)?;
		Ok(())
	}
}

#[macro_export]
macro_rules! directory {
	{ $($name:expr => $artifact:expr),* $(,)? } => {{
		let mut entries = ::std::collections::BTreeMap::new();
		$(
			entries.insert($name.into(), $artifact.into());
		)*
		$crate::Directory::with_entries(entries)
	}};
}
