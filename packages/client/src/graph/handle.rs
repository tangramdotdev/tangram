use {
	super::{Data, Id, Object},
	crate as tg,
	std::sync::Arc,
};

#[derive(Clone, Debug)]
pub struct Graph {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

impl Graph {
	#[must_use]
	pub fn with_nodes(nodes: Vec<tg::graph::Node>) -> Self {
		let object = tg::graph::Object { nodes };
		Self::with_object(object)
	}

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

	pub async fn nodes<H>(&self, handle: &H) -> tg::Result<Vec<tg::graph::Node>>
	where
		H: tg::Handle,
	{
		let object = self.load(handle).await?;
		Ok(object.nodes.clone())
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

	pub async fn get<H>(&self, handle: &H, index: usize) -> tg::Result<tg::Artifact>
	where
		H: tg::Handle,
	{
		let nodes = self.nodes(handle).await?;
		let node = nodes
			.get(index)
			.ok_or_else(|| tg::error!("invalid node index"))?;
		let artifact = match node.kind() {
			tg::artifact::Kind::Directory => {
				tg::Directory::with_graph_and_node(self.clone(), index).into()
			},
			tg::artifact::Kind::File => tg::File::with_graph_and_node(self.clone(), index).into(),
			tg::artifact::Kind::Symlink => {
				tg::Symlink::with_graph_and_node(self.clone(), index).into()
			},
		};
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
