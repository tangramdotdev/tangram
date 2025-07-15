use super::{Builder, Data, Id, Object};
use crate as tg;
use futures::{TryStreamExt, stream::FuturesUnordered};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::AsyncBufRead;

#[derive(Clone, Debug)]
pub struct File {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

impl File {
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
		let object = Object::try_from_data(data)?;
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
		Self::with_object(Object::Reference(tg::graph::object::Reference {
			graph: Some(graph),
			node,
		}))
	}

	pub async fn contents<H>(&self, handle: &H) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Reference(object) => {
				let graph = object.graph.as_ref().unwrap();
				let node = object.node;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				Ok(file.contents.clone())
			},
			Object::Node(object) => Ok(object.contents.clone()),
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
		let dependencies = match object.as_ref() {
			Object::Reference(reference) => {
				let graph = reference.graph.as_ref().unwrap();
				let node = reference.node;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				file.dependencies
					.clone()
					.into_iter()
					.map(async |(reference, referent)| {
						let object = match referent.item.clone() {
							tg::graph::object::Edge::Reference(reference) => {
								let (graph, object) = if let Some(graph) = reference.graph {
									let object = graph.object(handle).await?;
									(graph, object)
								} else {
									(graph.clone(), object.clone())
								};
								let node = object
									.nodes
									.get(reference.node)
									.ok_or_else(|| tg::error!("invalid index"))?;
								match node {
									tg::graph::Node::Directory(_) => {
										tg::Directory::with_graph_and_node(graph, reference.node)
											.into()
									},
									tg::graph::Node::File(_) => {
										tg::File::with_graph_and_node(graph, reference.node).into()
									},
									tg::graph::Node::Symlink(_) => {
										tg::Symlink::with_graph_and_node(graph, reference.node)
											.into()
									},
								}
							},
							tg::graph::object::Edge::Object(object) => object,
						};
						Ok::<_, tg::Error>((reference, referent.map(|_| object)))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?
			},
			Object::Node(node) => {
				node.dependencies
					.clone()
					.into_iter()
					.map(async |(reference, referent)| {
						let object: tg::Object = match referent.item.clone() {
							tg::graph::object::Edge::Reference(reference) => {
								let graph = reference
									.graph
									.ok_or_else(|| tg::error!("expected a graph"))?;
								let nodes = graph.nodes(handle).await?;
								let node = nodes
									.get(reference.node)
									.ok_or_else(|| tg::error!("invalid node index"))?;
								match node {
									tg::graph::Node::Directory(_) => {
										tg::Directory::with_graph_and_node(graph, reference.node)
											.into()
									},
									tg::graph::Node::File(_) => {
										tg::File::with_graph_and_node(graph, reference.node).into()
									},
									tg::graph::Node::Symlink(_) => {
										tg::Symlink::with_graph_and_node(graph, reference.node)
											.into()
									},
								}
							},
							tg::graph::object::Edge::Object(object) => object,
						};
						Ok::<_, tg::Error>((reference, referent.map(|_| object)))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?
			},
		};
		Ok(dependencies)
	}

	pub async fn get_dependency<H>(
		&self,
		handle: &H,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::Object>>
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
	) -> tg::Result<Option<tg::Referent<tg::Object>>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let referent = match object.as_ref() {
			Object::Reference(object) => {
				let graph = object.graph.as_ref().unwrap();
				let node = object.node;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let Some(referent) = file.dependencies.get(reference) else {
					return Ok(None);
				};
				let item = match referent.item.clone() {
					tg::graph::object::Edge::Reference(reference) => {
						let (graph, object) = if let Some(graph) = reference.graph {
							let object = graph.object(handle).await?;
							(graph, object)
						} else {
							(graph.clone(), object.clone())
						};
						match object.nodes.get(reference.node) {
							Some(tg::graph::Node::Directory(_)) => {
								tg::Directory::with_graph_and_node(graph, reference.node).into()
							},
							Some(tg::graph::Node::File(_)) => {
								tg::File::with_graph_and_node(graph, reference.node).into()
							},
							Some(tg::graph::Node::Symlink(_)) => {
								tg::Symlink::with_graph_and_node(graph, reference.node).into()
							},
							None => return Err(tg::error!("invalid index")),
						}
					},
					tg::graph::object::Edge::Object(object) => object,
				};
				tg::Referent {
					item,
					options: referent.options.clone(),
				}
			},
			Object::Node(node) => {
				let Some(referent) = node.dependencies.get(reference).cloned() else {
					return Ok(None);
				};
				match referent.item.clone() {
					tg::graph::object::Edge::Reference(reference) => {
						let graph = reference.graph.ok_or_else(|| tg::error!("missing graph"))?;
						let object = graph.object(handle).await?;
						let object = match object.nodes.get(reference.node) {
							Some(tg::graph::Node::Directory(_)) => {
								tg::Directory::with_graph_and_node(graph, reference.node).into()
							},
							Some(tg::graph::Node::File(_)) => {
								tg::File::with_graph_and_node(graph, reference.node).into()
							},
							Some(tg::graph::Node::Symlink(_)) => {
								tg::Symlink::with_graph_and_node(graph, reference.node).into()
							},
							None => return Err(tg::error!("invalid index")),
						};
						referent.map(|_| object)
					},
					tg::graph::object::Edge::Object(object) => referent.map(|_| object),
				}
			},
		};
		Ok(Some(referent))
	}

	pub async fn executable<H>(&self, handle: &H) -> tg::Result<bool>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Reference(object) => {
				let graph = object.graph.as_ref().unwrap();
				let node = object.node;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(node)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				Ok(file.executable)
			},
			Object::Node(node) => Ok(node.executable),
		}
	}

	pub async fn length<H>(&self, handle: &H) -> tg::Result<u64>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.length(handle).await
	}

	pub async fn read<H>(
		&self,
		handle: &H,
		arg: tg::blob::read::Arg,
	) -> tg::Result<impl AsyncBufRead + Send + use<H>>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.read(handle, arg).await
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

impl std::fmt::Display for File {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.file(self)?;
		Ok(())
	}
}

#[macro_export]
macro_rules! file {
	(@$builder:ident dependencies = $dependencies:expr $(, $($arg:tt)*)?) => {
		$builder = $builder.dependencies($dependencies);
		$crate::file!(@$builder $($($arg)*)?)
	};
	(@$builder:ident executable = $executable:expr $(, $($arg:tt)*)?) => {
		$builder = $builder.executable($executable);
		$crate::file!(@$builder $($($arg)*)?)
	};
	(@$builder:ident) => {};
	($contents:expr $(, $($arg:tt)*)?) => {{
		let mut builder = $crate::file::Builder::new($contents);
		$crate::file!(@builder $($($arg)*)?);
		builder.build()
	}};
}
