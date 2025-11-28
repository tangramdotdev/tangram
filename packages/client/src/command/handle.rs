use {
	super::{Builder, Data, Id, Object},
	crate::prelude::*,
	std::{collections::BTreeMap, ops::Deref, path::PathBuf, sync::Arc},
	tangram_util::arc::Ext as _,
};

#[derive(Clone, Debug)]
pub struct Command {
	state: Arc<std::sync::RwLock<State>>,
}

pub type State = tg::object::State<Id, Object>;

impl Command {
	pub fn builder(
		host: impl Into<String>,
		executable: impl Into<tg::command::Executable>,
	) -> Builder {
		Builder::new(host, executable)
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
			.ok_or_else(|| tg::error!(?self, "failed to load the object"))
	}

	pub async fn try_load<H>(&self, handle: &H) -> tg::Result<Option<Arc<Object>>>
	where
		H: tg::Handle,
	{
		if let Some(object) = self.state.read().unwrap().object.clone() {
			return Ok(Some(object));
		}
		let id = self.state.read().unwrap().id.clone().unwrap();
		let arg = tg::object::get::Arg::default();
		let Some(output) = handle.try_get_object(&id.into(), arg).await? else {
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
		let mut state = self.state.write().unwrap();
		if state.stored {
			state.object.take();
		}
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

impl Command {
	pub async fn args<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = Vec<tg::Value>> + use<H>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.args))
	}

	pub async fn cwd<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = Option<PathBuf>> + use<H>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.cwd))
	}

	pub async fn env<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = BTreeMap<String, tg::Value>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.env))
	}

	pub async fn executable<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = tg::command::Executable>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.executable))
	}

	pub async fn host<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = String>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.host))
	}

	pub async fn mounts<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = Vec<tg::command::Mount>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.mounts))
	}

	pub async fn stdin<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = Option<tg::Blob>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.stdin))
	}

	pub async fn user<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = Option<String>>>
	where
		H: tg::Handle,
	{
		Ok(self.object(handle).await?.map(|object| &object.user))
	}
}

impl std::fmt::Display for Command {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.command(self)?;
		Ok(())
	}
}
