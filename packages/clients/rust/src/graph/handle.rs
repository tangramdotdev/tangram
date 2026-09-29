use {
	super::{Builder, Data, Id, Object},
	crate::prelude::*,
	std::sync::Arc,
};

#[derive(Clone, Debug)]
pub struct Graph {
	state: tg::object::State,
}

impl Graph {
	#[must_use]
	pub fn builder() -> Builder {
		Builder::new()
	}

	#[must_use]
	pub fn with_nodes(nodes: Vec<tg::graph::Node>) -> Self {
		Self::builder().nodes(nodes).build()
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
		let graph = Self::with_id(referent.node);
		graph.state().set_location(referent.options.location);
		graph.state().set_tokens(referent.options.tokens);

		graph
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
		let object = object.unwrap_graph_ref().clone();
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

	pub async fn nodes(&self) -> tg::Result<Vec<tg::graph::Node>> {
		let instance = tg::instance()?;
		self.nodes_with_instance(instance).await
	}

	pub async fn nodes_with_instance<I>(&self, instance: &I) -> tg::Result<Vec<tg::graph::Node>>
	where
		I: tg::Instance,
	{
		let object = self.load_with_instance(instance).await?;
		Ok(object.nodes.clone())
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

	pub async fn get(&self, index: usize) -> tg::Result<tg::Artifact> {
		let instance = tg::instance()?;
		self.get_with_instance(instance, index).await
	}

	pub async fn get_with_instance<I>(&self, instance: &I, index: usize) -> tg::Result<tg::Artifact>
	where
		I: tg::Instance,
	{
		let edge = self.get_edge_with_instance(instance, index).await?;
		let artifact = tg::Artifact::with_edge(edge)?;
		artifact.inherit_location(self.state.location().as_ref());
		artifact.inherit_tokens(&self.state.tokens());

		Ok(artifact)
	}

	pub async fn get_edge_with_instance<I>(
		&self,
		instance: &I,
		index: usize,
	) -> tg::Result<tg::graph::Edge<tg::Artifact>>
	where
		I: tg::Instance,
	{
		let object = self.object_with_instance(instance).await?;
		let node = object
			.nodes
			.get(index)
			.ok_or_else(|| tg::error!("invalid node index"))?;
		let edge = tg::graph::Edge::Pointer(tg::graph::Pointer {
			graph: self.clone(),
			index,
			kind: node.kind(),
		});

		Ok(edge)
	}
}

impl std::fmt::Display for Graph {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.graph(self)?;
		Ok(())
	}
}
