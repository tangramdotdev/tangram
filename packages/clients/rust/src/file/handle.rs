use {
	super::{Builder, Data, Id, Object},
	crate::prelude::*,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::{collections::BTreeMap, sync::Arc},
	tokio::io::AsyncBufRead,
};

#[derive(Clone, Debug)]
pub struct File {
	state: tg::object::State,
}

impl File {
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
		let object = object.unwrap_file_ref().clone();
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
	pub fn with_pointer(pointer: tg::graph::Pointer) -> Self {
		Self::with_object(Object::Pointer(pointer))
	}

	#[must_use]
	pub fn with_edge(edge: tg::graph::Edge<Self>) -> Self {
		match edge {
			tg::graph::Edge::Pointer(pointer) => Self::with_pointer(pointer),
			tg::graph::Edge::Object(file) => file,
		}
	}

	pub async fn contents<H>(&self, handle: &H) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Pointer(object) => {
				let graph = object.graph.as_ref().unwrap();
				let index = object.index;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(index)
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
	) -> tg::Result<BTreeMap<tg::Reference, Option<tg::file::Dependency>>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let dependencies = match object.as_ref() {
			Object::Pointer(pointer) => {
				let graph = pointer.graph.as_ref().unwrap();
				let index = pointer.index;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				file.dependencies
					.clone()
					.into_iter()
					.map(async |(reference, option)| {
						let option = 'a: {
							let Some(dependency) = &option else {
								break 'a None;
							};
							let object = match dependency.0.item.clone() {
								Some(tg::graph::Edge::Pointer(pointer)) => {
									let graph = pointer.graph.unwrap_or_else(|| graph.clone());
									tg::Artifact::with_pointer(tg::graph::Pointer {
										graph: Some(graph),
										index: pointer.index,
										kind: pointer.kind,
									})
									.into()
								},
								Some(tg::graph::Edge::Object(object)) => object,
								None => {
									break 'a Some(tg::file::Dependency(
										dependency.0.clone().map(|_| None),
									));
								},
							};
							Some(tg::file::Dependency(
								dependency.0.clone().map(|_| Some(object)),
							))
						};
						Ok::<_, tg::Error>((reference, option))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?
			},
			Object::Node(node) => {
				node.dependencies
					.clone()
					.into_iter()
					.map(async |(reference, option)| {
						let option = 'a: {
							let Some(dependency) = &option else {
								break 'a None;
							};
							let object: tg::Object = match dependency.0.item.clone() {
								Some(tg::graph::Edge::Pointer(pointer)) => {
									let graph = pointer
										.graph
										.ok_or_else(|| tg::error!("expected a graph"))?;
									tg::Artifact::with_pointer(tg::graph::Pointer {
										graph: Some(graph),
										index: pointer.index,
										kind: pointer.kind,
									})
									.into()
								},
								Some(tg::graph::Edge::Object(object)) => object,
								None => {
									break 'a Some(tg::file::Dependency(
										dependency.0.clone().map(|_| None),
									));
								},
							};
							Some(tg::file::Dependency(
								dependency.0.clone().map(|_| Some(object)),
							))
						};
						Ok::<_, tg::Error>((reference, option))
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
	) -> tg::Result<tg::file::Dependency>
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
	) -> tg::Result<Option<tg::file::Dependency>>
	where
		H: tg::Handle,
	{
		let Some(dependency) = self.try_get_dependency_edge(handle, reference).await? else {
			return Ok(None);
		};
		let item = match dependency.0.item {
			Some(tg::graph::Edge::Pointer(pointer)) => {
				Some(tg::Artifact::with_pointer(pointer).into())
			},
			Some(tg::graph::Edge::Object(object)) => Some(object),
			None => None,
		};
		Ok(Some(tg::file::Dependency(tg::Referent {
			item,
			options: dependency.0.options,
		})))
	}

	pub async fn get_dependency_edge<H>(
		&self,
		handle: &H,
		reference: &tg::Reference,
	) -> tg::Result<tg::graph::Dependency>
	where
		H: tg::Handle,
	{
		self.try_get_dependency_edge(handle, reference)
			.await?
			.ok_or_else(|| tg::error!("expected the dependency to exist"))
	}

	pub async fn try_get_dependency_edge<H>(
		&self,
		handle: &H,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::graph::Dependency>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		let dependency = match object.as_ref() {
			Object::Pointer(pointer) => {
				let graph = pointer.graph.as_ref().unwrap();
				let index = pointer.index;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let Some(dependency) = file.dependencies.get(reference).ok_or_else(
					|| tg::error!(file = %self.id(), %reference, "expected a dependency"),
				)?
				else {
					return Ok(None);
				};
				let item = match dependency.0.item.clone() {
					Some(tg::graph::Edge::Pointer(pointer)) => {
						let graph = pointer.graph.unwrap_or_else(|| graph.clone());
						Some(tg::graph::Edge::Pointer(tg::graph::Pointer {
							graph: Some(graph),
							index: pointer.index,
							kind: pointer.kind,
						}))
					},
					Some(tg::graph::Edge::Object(object)) => Some(tg::graph::Edge::Object(object)),
					None => None,
				};
				tg::graph::Dependency(tg::Referent {
					item,
					options: dependency.0.options.clone(),
				})
			},
			Object::Node(node) => {
				let Some(dependency) = node.dependencies.get(reference).ok_or_else(
					|| tg::error!(file = %self.id(), %reference, "expected a dependency"),
				)?
				else {
					return Ok(None);
				};
				let item = match dependency.0.item.clone() {
					Some(tg::graph::Edge::Pointer(pointer)) => {
						let graph = pointer.graph.ok_or_else(|| tg::error!("missing graph"))?;
						Some(tg::graph::Edge::Pointer(tg::graph::Pointer {
							graph: Some(graph),
							index: pointer.index,
							kind: pointer.kind,
						}))
					},
					Some(tg::graph::Edge::Object(object)) => Some(tg::graph::Edge::Object(object)),
					None => None,
				};
				tg::graph::Dependency(tg::Referent {
					item,
					options: dependency.0.options.clone(),
				})
			},
		};
		Ok(Some(dependency))
	}

	pub async fn executable<H>(&self, handle: &H) -> tg::Result<bool>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Pointer(object) => {
				let graph = object.graph.as_ref().unwrap();
				let index = object.index;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(index)
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

	pub async fn module<H>(&self, handle: &H) -> tg::Result<Option<tg::module::Kind>>
	where
		H: tg::Handle,
	{
		let object = self.object(handle).await?;
		match object.as_ref() {
			Object::Pointer(object) => {
				let graph = object.graph.as_ref().unwrap();
				let index = object.index;
				let object = graph.object(handle).await?;
				let node = object
					.nodes
					.get(index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				Ok(file.module)
			},
			Object::Node(node) => Ok(node.module),
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
		options: tg::read::Options,
	) -> tg::Result<impl AsyncBufRead + Send + use<H>>
	where
		H: tg::Handle,
	{
		self.contents(handle).await?.read(handle, options).await
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
