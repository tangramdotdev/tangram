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
	pub fn try_with_referent<T>(referent: tg::Referent<T>) -> std::result::Result<Self, T::Error>
	where
		T: TryInto<Id>,
	{
		let referent = referent.try_map(TryInto::try_into)?;

		Ok(Self::with_referent(referent))
	}

	#[must_use]
	pub fn with_referent(referent: tg::Referent<Id>) -> Self {
		let file = Self::with_id(referent.node);
		file.state().set_location(referent.options.location);
		file.state().set_tokens(referent.options.tokens);

		file
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
	pub fn with_state(state: tg::object::State) -> Self {
		Self { state }
	}

	#[must_use]
	pub fn state(&self) -> &tg::object::State {
		&self.state
	}

	#[must_use]
	pub fn id(&self) -> Id {
		self.state.id().try_into().unwrap()
	}

	#[must_use]
	pub fn to_referent(&self) -> tg::Referent<Id> {
		let options = tg::referent::Options {
			location: self.state.location(),
			tokens: self.state.collect_tokens(),
			..tg::referent::Options::default()
		};

		tg::Referent::new(self.id(), options)
	}

	pub async fn object(&self) -> tg::Result<Arc<Object>> {
		let instance = tg::instance()?;
		self.object_with_instance(instance).await
	}

	pub async fn object_with_instance<I>(&self, instance: &I) -> tg::Result<Arc<Object>>
	where
		I: tg::Instance,
	{
		self.load_with_instance(instance).await
	}

	pub async fn load(&self) -> tg::Result<Arc<Object>> {
		let instance = tg::instance()?;
		self.load_with_instance(instance).await
	}

	pub async fn load_with_instance<I>(&self, instance: &I) -> tg::Result<Arc<Object>>
	where
		I: tg::Instance,
	{
		self.try_load_with_instance(instance)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<Object>>> {
		let instance = tg::instance()?;
		self.try_load_with_instance(instance).await
	}

	pub async fn try_load_with_instance<I>(&self, instance: &I) -> tg::Result<Option<Arc<Object>>>
	where
		I: tg::Instance,
	{
		self.try_load_with_arg_with_instance(instance, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<Arc<Object>> {
		let instance = tg::instance()?;
		self.load_with_arg_with_instance(instance, arg).await
	}

	pub async fn load_with_arg_with_instance<I>(
		&self,
		instance: &I,
		arg: tg::object::get::Arg,
	) -> tg::Result<Arc<Object>>
	where
		I: tg::Instance,
	{
		self.try_load_with_arg_with_instance(instance, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load_with_arg(
		&self,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>> {
		let instance = tg::instance()?;
		self.try_load_with_arg_with_instance(instance, arg).await
	}

	pub async fn try_load_with_arg_with_instance<I>(
		&self,
		instance: &I,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>>
	where
		I: tg::Instance,
	{
		let object = self
			.state
			.try_load_with_arg_with_instance(instance, arg)
			.await?;
		let Some(object) = object else {
			return Ok(None);
		};
		let object = object.unwrap_file_ref().clone();
		Ok(Some(object))
	}

	pub fn unload(&self) {
		self.state.unload();
	}

	pub async fn store(&self) -> tg::Result<Id> {
		let instance = tg::instance()?;
		self.store_with_instance(instance).await
	}

	pub async fn store_with_instance<I>(&self, instance: &I) -> tg::Result<Id>
	where
		I: tg::Instance,
	{
		tg::Value::from(self.clone())
			.store_with_instance(instance)
			.await?;
		Ok(self.id())
	}

	pub async fn children(&self) -> tg::Result<Vec<tg::Object>> {
		let instance = tg::instance()?;
		self.children_with_instance(instance).await
	}

	pub async fn children_with_instance<I>(&self, instance: &I) -> tg::Result<Vec<tg::Object>>
	where
		I: tg::Instance,
	{
		self.state.children_with_instance(instance).await
	}

	pub async fn data(&self) -> tg::Result<Data> {
		let instance = tg::instance()?;
		self.data_with_instance(instance).await
	}

	pub async fn data_with_instance<I>(&self, instance: &I) -> tg::Result<Data>
	where
		I: tg::Instance,
	{
		Ok(self.object_with_instance(instance).await?.to_data())
	}
}

impl File {
	#[must_use]
	pub fn builder() -> Builder {
		Builder::new()
	}

	#[must_use]
	pub fn with_contents(contents: impl Into<tg::Blob>) -> Self {
		Self::builder().contents(contents).build().unwrap()
	}

	#[must_use]
	pub fn with_pointer(pointer: tg::graph::Pointer) -> Self {
		Self::with_object(Object::Pointer(pointer))
	}

	pub fn with_edge(edge: tg::graph::Edge<Self>) -> tg::Result<Self> {
		match edge {
			tg::graph::Edge::Index(_) => Err(tg::error!("missing graph")),
			tg::graph::Edge::Pointer(pointer) => Ok(Self::with_pointer(pointer)),
			tg::graph::Edge::Object(file) => Ok(file),
		}
	}

	pub async fn contents(&self) -> tg::Result<tg::Blob> {
		let instance = tg::instance()?;
		self.contents_with_instance(instance).await
	}

	pub async fn contents_with_instance<I>(&self, instance: &I) -> tg::Result<tg::Blob>
	where
		I: tg::Instance,
	{
		let object = self.object_with_instance(instance).await?;
		let contents = match object.as_ref() {
			Object::Pointer(object) => {
				let graph = &object.graph;
				let index = object.index;
				let object = graph.object_with_instance(instance).await?;
				let node = object
					.nodes
					.get(index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				file.contents.clone()
			},
			Object::Node(object) => object.contents.clone(),
		};

		contents
			.state()
			.inherit_location(self.state.location().as_ref());
		contents.state().inherit_tokens(&self.state.tokens());

		Ok(contents)
	}

	pub async fn dependencies(
		&self,
	) -> tg::Result<BTreeMap<tg::Reference, Option<tg::file::Dependency>>> {
		let instance = tg::instance()?;
		self.dependencies_with_instance(instance).await
	}

	pub async fn dependencies_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<BTreeMap<tg::Reference, Option<tg::file::Dependency>>>
	where
		I: tg::Instance,
	{
		let object = self.object_with_instance(instance).await?;
		let location = self.state.location();
		let tokens = self.state.tokens();
		let dependencies = match object.as_ref() {
			Object::Pointer(pointer) => {
				let graph = &pointer.graph;
				let index = pointer.index;
				let object = graph.object_with_instance(instance).await?;
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
							let object = match dependency.0.node.clone() {
								Some(tg::graph::Edge::Index(index)) => {
									graph.get_with_instance(instance, index).await?.into()
								},
								Some(tg::graph::Edge::Pointer(pointer)) => {
									tg::Artifact::with_pointer(pointer).into()
								},
								Some(tg::graph::Edge::Object(object)) => object,
								None => {
									break 'a Some(tg::file::Dependency(
										dependency.0.clone().map(|_| None),
									));
								},
							};
							object.inherit_location(
								dependency.0.options.location.as_ref().or(location.as_ref()),
							);
							object.inherit_tokens(&dependency.0.options.tokens);
							object.inherit_tokens(&tokens);
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
							let object: tg::Object = match dependency.0.node.clone() {
								Some(tg::graph::Edge::Index(_index)) => {
									return Err(tg::error!("missing graph"));
								},
								Some(tg::graph::Edge::Pointer(pointer)) => {
									tg::Artifact::with_pointer(pointer).into()
								},
								Some(tg::graph::Edge::Object(object)) => object,
								None => {
									break 'a Some(tg::file::Dependency(
										dependency.0.clone().map(|_| None),
									));
								},
							};
							object.inherit_location(
								dependency.0.options.location.as_ref().or(location.as_ref()),
							);
							object.inherit_tokens(&dependency.0.options.tokens);
							object.inherit_tokens(&tokens);
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

	pub async fn get_dependency(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<tg::file::Dependency> {
		let instance = tg::instance()?;
		self.get_dependency_with_instance(instance, reference).await
	}

	pub async fn get_dependency_with_instance<I>(
		&self,
		instance: &I,
		reference: &tg::Reference,
	) -> tg::Result<tg::file::Dependency>
	where
		I: tg::Instance,
	{
		self.try_get_dependency_with_instance(instance, reference)
			.await?
			.ok_or_else(|| tg::error!("expected the dependency to exist"))
	}

	pub async fn try_get_dependency(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::file::Dependency>> {
		let instance = tg::instance()?;
		self.try_get_dependency_with_instance(instance, reference)
			.await
	}

	pub async fn try_get_dependency_with_instance<I>(
		&self,
		instance: &I,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::file::Dependency>>
	where
		I: tg::Instance,
	{
		let Some(dependency) = self
			.try_get_dependency_edge_with_instance(instance, reference)
			.await?
		else {
			return Ok(None);
		};
		let node = match dependency.0.node {
			Some(tg::graph::Edge::Index(_)) => return Err(tg::error!("missing graph")),
			Some(tg::graph::Edge::Pointer(pointer)) => {
				let object: tg::Object = tg::Artifact::with_pointer(pointer).into();
				object.inherit_location(
					dependency
						.0
						.options
						.location
						.as_ref()
						.or(self.state.location().as_ref()),
				);
				object.inherit_tokens(&dependency.0.options.tokens);
				object.inherit_tokens(&self.state.tokens());
				Some(object)
			},
			Some(tg::graph::Edge::Object(object)) => {
				object.inherit_location(
					dependency
						.0
						.options
						.location
						.as_ref()
						.or(self.state.location().as_ref()),
				);
				object.inherit_tokens(&dependency.0.options.tokens);
				object.inherit_tokens(&self.state.tokens());
				Some(object)
			},
			None => None,
		};
		Ok(Some(tg::file::Dependency(tg::Referent {
			node,
			options: dependency.0.options,
		})))
	}

	pub async fn get_dependency_edge(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<tg::graph::Dependency> {
		let instance = tg::instance()?;
		self.get_dependency_edge_with_instance(instance, reference)
			.await
	}

	pub async fn get_dependency_edge_with_instance<I>(
		&self,
		instance: &I,
		reference: &tg::Reference,
	) -> tg::Result<tg::graph::Dependency>
	where
		I: tg::Instance,
	{
		self.try_get_dependency_edge_with_instance(instance, reference)
			.await?
			.ok_or_else(|| tg::error!("expected the dependency to exist"))
	}

	pub async fn try_get_dependency_edge(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::graph::Dependency>> {
		let instance = tg::instance()?;
		self.try_get_dependency_edge_with_instance(instance, reference)
			.await
	}

	pub async fn try_get_dependency_edge_with_instance<I>(
		&self,
		instance: &I,
		reference: &tg::Reference,
	) -> tg::Result<Option<tg::graph::Dependency>>
	where
		I: tg::Instance,
	{
		let reference = reference.without_token();
		let object = self.object_with_instance(instance).await?;
		let dependency = match object.as_ref() {
			Object::Pointer(pointer) => {
				let graph = &pointer.graph;
				let index = pointer.index;
				let object = graph.object_with_instance(instance).await?;
				let node = object
					.nodes
					.get(index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let file = node
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let Some(dependency) = file.dependencies.get(&reference).ok_or_else(
					|| tg::error!(file = %self.id(), node = %reference.node(), "expected a dependency"),
				)?
				else {
					return Ok(None);
				};
				let node = match dependency.0.node.clone() {
					Some(tg::graph::Edge::Index(index)) => {
						Some(graph.get_edge_with_instance(instance, index).await?.into())
					},
					Some(tg::graph::Edge::Pointer(pointer)) => {
						Some(tg::graph::Edge::Pointer(pointer))
					},
					Some(tg::graph::Edge::Object(object)) => Some(tg::graph::Edge::Object(object)),
					None => None,
				};
				tg::graph::Dependency(tg::Referent {
					node,
					options: dependency.0.options.clone(),
				})
			},
			Object::Node(node) => {
				let Some(dependency) = node.dependencies.get(&reference).ok_or_else(
					|| tg::error!(file = %self.id(), node = %reference.node(), "expected a dependency"),
				)?
				else {
					return Ok(None);
				};
				let node = match dependency.0.node.clone() {
					Some(tg::graph::Edge::Index(_index)) => {
						return Err(tg::error!("missing graph"));
					},
					Some(tg::graph::Edge::Pointer(pointer)) => {
						Some(tg::graph::Edge::Pointer(pointer))
					},
					Some(tg::graph::Edge::Object(object)) => Some(tg::graph::Edge::Object(object)),
					None => None,
				};
				tg::graph::Dependency(tg::Referent {
					node,
					options: dependency.0.options.clone(),
				})
			},
		};
		Ok(Some(dependency))
	}

	pub async fn executable(&self) -> tg::Result<bool> {
		let instance = tg::instance()?;
		self.executable_with_instance(instance).await
	}

	pub async fn executable_with_instance<I>(&self, instance: &I) -> tg::Result<bool>
	where
		I: tg::Instance,
	{
		let object = self.object_with_instance(instance).await?;
		match object.as_ref() {
			Object::Pointer(object) => {
				let graph = &object.graph;
				let index = object.index;
				let object = graph.object_with_instance(instance).await?;
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

	pub async fn module(&self) -> tg::Result<Option<tg::module::Kind>> {
		let instance = tg::instance()?;
		self.module_with_instance(instance).await
	}

	pub async fn module_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<Option<tg::module::Kind>>
	where
		I: tg::Instance,
	{
		let object = self.object_with_instance(instance).await?;
		match object.as_ref() {
			Object::Pointer(object) => {
				let graph = &object.graph;
				let index = object.index;
				let object = graph.object_with_instance(instance).await?;
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

	pub async fn length(&self) -> tg::Result<u64> {
		let instance = tg::instance()?;
		self.length_with_instance(instance).await
	}

	pub async fn length_with_instance<I>(&self, instance: &I) -> tg::Result<u64>
	where
		I: tg::Instance,
	{
		self.contents_with_instance(instance)
			.await?
			.length_with_instance(instance)
			.await
	}

	pub async fn read(&self, options: tg::read::Options) -> tg::Result<impl AsyncBufRead + Send> {
		let instance = tg::instance()?;
		self.read_with_instance(instance, options).await
	}

	pub async fn read_with_instance<I>(
		&self,
		instance: &I,
		options: tg::read::Options,
	) -> tg::Result<impl AsyncBufRead + Send + use<I>>
	where
		I: tg::Instance,
	{
		self.contents_with_instance(instance)
			.await?
			.read_with_instance(instance, options)
			.await
	}

	pub async fn bytes(&self) -> tg::Result<Vec<u8>> {
		let instance = tg::instance()?;
		self.bytes_with_instance(instance).await
	}

	pub async fn bytes_with_instance<I>(&self, instance: &I) -> tg::Result<Vec<u8>>
	where
		I: tg::Instance,
	{
		self.contents_with_instance(instance)
			.await?
			.bytes_with_instance(instance)
			.await
	}

	pub async fn text(&self) -> tg::Result<String> {
		let instance = tg::instance()?;
		self.text_with_instance(instance).await
	}

	pub async fn text_with_instance<I>(&self, instance: &I) -> tg::Result<String>
	where
		I: tg::Instance,
	{
		self.contents_with_instance(instance)
			.await?
			.text_with_instance(instance)
			.await
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
