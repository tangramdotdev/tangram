use {
	super::{Builder, Data, Id, Object},
	crate::prelude::*,
	std::{path::PathBuf, sync::Arc},
};

#[derive(Clone, Debug)]
pub struct Symlink {
	state: tg::object::State,
}

impl Symlink {
	#[must_use]
	pub fn builder() -> Builder {
		Builder::new()
	}

	pub fn try_with_referent<T>(referent: tg::Referent<T>) -> std::result::Result<Self, T::Error>
	where
		T: TryInto<Id>,
	{
		let referent = referent.try_map(TryInto::try_into)?;

		Ok(Self::with_referent(referent))
	}

	#[must_use]
	pub fn with_referent(referent: tg::Referent<Id>) -> Self {
		let symlink = Self::with_id(referent.node);
		symlink.state().set_location(referent.options.location);
		symlink.state().set_tokens(referent.options.tokens);

		symlink
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
		let object = object.unwrap_symlink_ref().clone();
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

impl Symlink {
	#[must_use]
	pub fn with_pointer(pointer: tg::graph::Pointer) -> Self {
		Self::with_object(Object::Pointer(pointer))
	}

	pub fn with_edge(edge: tg::graph::Edge<Self>) -> tg::Result<Self> {
		match edge {
			tg::graph::Edge::Index(_) => Err(tg::error!("missing graph")),
			tg::graph::Edge::Pointer(pointer) => Ok(Self::with_pointer(pointer)),
			tg::graph::Edge::Object(symlink) => Ok(symlink),
		}
	}

	#[must_use]
	pub fn with_artifact_and_path(artifact: tg::Artifact, path: PathBuf) -> Self {
		Self::builder()
			.artifact(artifact)
			.path(path)
			.build()
			.unwrap()
	}

	#[must_use]
	pub fn with_artifact(artifact: tg::Artifact) -> Self {
		Self::builder().artifact(artifact).build().unwrap()
	}

	#[must_use]
	pub fn with_path(path: PathBuf) -> Self {
		Self::builder().path(path).build().unwrap()
	}

	pub async fn artifact(&self) -> tg::Result<Option<tg::Artifact>> {
		let instance = tg::instance()?;
		self.artifact_with_instance(instance).await
	}

	pub async fn artifact_with_instance<I>(&self, instance: &I) -> tg::Result<Option<tg::Artifact>>
	where
		I: tg::Instance,
	{
		let object = self.object_with_instance(instance).await?;
		let artifact = match object.as_ref() {
			Object::Pointer(object) => {
				let graph = &object.graph;
				let index = object.index;
				let object = graph.object_with_instance(instance).await?;
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
					tg::graph::Edge::Index(index) => {
						graph.get_with_instance(instance, *index).await?
					},
					tg::graph::Edge::Pointer(pointer) => {
						tg::Artifact::with_pointer(pointer.clone())
					},
					tg::graph::Edge::Object(object) => object.clone(),
				};
				Some(artifact)
			},
			Object::Node(node) => {
				let Some(artifact) = &node.artifact else {
					return Ok(None);
				};
				let artifact = match artifact.clone() {
					tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
					tg::graph::Edge::Pointer(pointer) => tg::Artifact::with_pointer(pointer),
					tg::graph::Edge::Object(object) => object.clone(),
				};
				Some(artifact)
			},
		};
		if let Some(artifact) = &artifact {
			artifact.inherit_location(self.state.location().as_ref());
			artifact.inherit_tokens(&self.state.tokens());
		}
		Ok(artifact)
	}

	pub async fn path(&self) -> tg::Result<Option<PathBuf>> {
		let instance = tg::instance()?;
		self.path_with_instance(instance).await
	}

	pub async fn path_with_instance<I>(&self, instance: &I) -> tg::Result<Option<PathBuf>>
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
				let symlink = node
					.try_unwrap_symlink_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				Ok(symlink.path.clone())
			},
			Object::Node(node) => Ok(node.path.clone()),
		}
	}

	pub async fn resolve(&self) -> tg::Result<tg::Artifact> {
		let instance = tg::instance()?;
		self.resolve_with_instance(instance).await
	}

	pub async fn resolve_with_instance<I>(&self, instance: &I) -> tg::Result<tg::Artifact>
	where
		I: tg::Instance,
	{
		self.try_resolve_with_instance(instance)
			.await?
			.ok_or_else(|| tg::error!("broken symlink"))
	}

	pub async fn try_resolve(&self) -> tg::Result<Option<tg::Artifact>> {
		let instance = tg::instance()?;
		self.try_resolve_with_instance(instance).await
	}

	pub async fn try_resolve_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<Option<tg::Artifact>>
	where
		I: tg::Instance,
	{
		let mut artifact = self.artifact_with_instance(instance).await?.clone();
		if let Some(tg::Artifact::Symlink(symlink)) = artifact {
			artifact = Box::pin(symlink.try_resolve_with_instance(instance)).await?;
		}
		let path = self.path_with_instance(instance).await?.clone();
		match (artifact, path) {
			(None, Some(_)) => Err(tg::error!("cannot resolve a symlink with no artifact")),
			(Some(artifact), None) => Ok(Some(artifact)),
			(Some(tg::Artifact::Directory(directory)), Some(path)) => {
				directory.try_get_with_instance(instance, path).await
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
