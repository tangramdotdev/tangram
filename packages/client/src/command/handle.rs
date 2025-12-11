use {
	super::{Builder, Data, Id, Object},
	crate::prelude::*,
	std::{collections::BTreeMap, ops::Deref, path::PathBuf, sync::Arc},
	tangram_util::arc::Ext as _,
};

#[derive(Clone, Debug)]
pub struct Command {
	state: tg::object::State,
}

impl Command {
	pub fn builder(
		host: impl Into<String>,
		executable: impl Into<tg::command::Executable>,
	) -> Builder {
		Builder::new(host, executable)
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
		let object = object.unwrap_command_ref().clone();
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
