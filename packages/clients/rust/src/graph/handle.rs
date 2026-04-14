use {
	super::{Data, Id, Object},
	crate::prelude::*,
	std::sync::Arc,
};

#[derive(Clone, Debug)]
pub struct Graph {
	state: tg::object::State,
}

impl Graph {
	#[must_use]
	pub fn with_nodes(nodes: Vec<tg::graph::Node>) -> Self {
		let object = tg::graph::Object { nodes };
		Self::with_object(object)
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

	pub async fn object(&self) -> tg::Result<Arc<Object>> {
		let handle = tg::handle()?;
		self.object_with_handle(handle).await
	}

	pub async fn object_with_handle<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.load_with_handle(handle).await
	}

	pub async fn load(&self) -> tg::Result<Arc<Object>> {
		let handle = tg::handle()?;
		self.load_with_handle(handle).await
	}

	pub async fn load_with_handle<H>(&self, handle: &H) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_handle(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<Object>>> {
		let handle = tg::handle()?;
		self.try_load_with_handle(handle).await
	}

	pub async fn try_load_with_handle<H>(&self, handle: &H) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg_with_handle(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn load_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<Arc<Object>> {
		let handle = tg::handle()?;
		self.load_with_arg_with_handle(handle, arg).await
	}

	pub async fn load_with_arg_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Arc<Object>>
	where
		H: tg::Handle,
	{
		self.try_load_with_arg_with_handle(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to load the object"))
	}

	pub async fn try_load_with_arg(
		&self,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>> {
		let handle = tg::handle()?;
		self.try_load_with_arg_with_handle(handle, arg).await
	}

	pub async fn try_load_with_arg_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		let object = self
			.state
			.try_load_with_arg_with_handle(handle, arg)
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
		let handle = tg::handle()?;
		self.store_with_handle(handle).await
	}

	pub async fn store_with_handle<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		tg::Value::from(self.clone())
			.store_with_handle(handle)
			.await?;
		Ok(self.id())
	}

	pub async fn nodes(&self) -> tg::Result<Vec<tg::graph::Node>> {
		let handle = tg::handle()?;
		self.nodes_with_handle(handle).await
	}

	pub async fn nodes_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<tg::graph::Node>>
	where
		H: tg::Handle,
	{
		let object = self.load_with_handle(handle).await?;
		Ok(object.nodes.clone())
	}

	pub async fn children(&self) -> tg::Result<Vec<tg::Object>> {
		let handle = tg::handle()?;
		self.children_with_handle(handle).await
	}

	pub async fn children_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<tg::Object>>
	where
		H: tg::Handle,
	{
		self.state.children_with_handle(handle).await
	}

	pub async fn data(&self) -> tg::Result<Data> {
		let handle = tg::handle()?;
		self.data_with_handle(handle).await
	}

	pub async fn data_with_handle<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		Ok(self.object_with_handle(handle).await?.to_data())
	}

	pub async fn get(&self, index: usize) -> tg::Result<tg::Artifact> {
		let handle = tg::handle()?;
		self.get_with_handle(handle, index).await
	}

	pub async fn get_with_handle<H>(&self, handle: &H, index: usize) -> tg::Result<tg::Artifact>
	where
		H: tg::Handle,
	{
		let nodes = self.nodes_with_handle(handle).await?;
		let node = nodes
			.get(index)
			.ok_or_else(|| tg::error!("invalid node index"))?;
		let artifact = tg::Artifact::with_pointer(tg::graph::Pointer {
			graph: Some(self.clone()),
			index,
			kind: node.kind(),
		});
		Ok(artifact)
	}
}

impl std::fmt::Display for Graph {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.graph(self)?;
		Ok(())
	}
}
