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
	#[must_use]
	pub fn builder() -> Builder {
		Builder::new()
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
		let object = object.unwrap_command_ref().clone();
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
}

impl Command {
	pub async fn args(&self) -> tg::Result<impl Deref<Target = Vec<tg::Value>>> {
		let handle = tg::handle()?;
		self.args_with_handle(handle).await
	}

	pub async fn args_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = Vec<tg::Value>> + use<H>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object_with_handle(handle)
			.await?
			.map(|object| &object.args))
	}

	pub async fn cwd(&self) -> tg::Result<impl Deref<Target = Option<PathBuf>>> {
		let handle = tg::handle()?;
		self.cwd_with_handle(handle).await
	}

	pub async fn cwd_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = Option<PathBuf>> + use<H>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object_with_handle(handle)
			.await?
			.map(|object| &object.cwd))
	}

	pub async fn env(&self) -> tg::Result<impl Deref<Target = BTreeMap<String, tg::Value>>> {
		let handle = tg::handle()?;
		self.env_with_handle(handle).await
	}

	pub async fn env_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = BTreeMap<String, tg::Value>>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object_with_handle(handle)
			.await?
			.map(|object| &object.env))
	}

	pub async fn executable(&self) -> tg::Result<impl Deref<Target = tg::command::Executable>> {
		let handle = tg::handle()?;
		self.executable_with_handle(handle).await
	}

	pub async fn executable_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = tg::command::Executable>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object_with_handle(handle)
			.await?
			.map(|object| &object.executable))
	}

	pub async fn host(&self) -> tg::Result<impl Deref<Target = String>> {
		let handle = tg::handle()?;
		self.host_with_handle(handle).await
	}

	pub async fn host_with_handle<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = String>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object_with_handle(handle)
			.await?
			.map(|object| &object.host))
	}

	pub async fn stdin(&self) -> tg::Result<impl Deref<Target = Option<tg::Blob>>> {
		let handle = tg::handle()?;
		self.stdin_with_handle(handle).await
	}

	pub async fn stdin_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = Option<tg::Blob>>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object_with_handle(handle)
			.await?
			.map(|object| &object.stdin))
	}

	pub async fn user(&self) -> tg::Result<impl Deref<Target = Option<String>>> {
		let handle = tg::handle()?;
		self.user_with_handle(handle).await
	}

	pub async fn user_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = Option<String>>>
	where
		H: tg::Handle,
	{
		Ok(self
			.object_with_handle(handle)
			.await?
			.map(|object| &object.user))
	}
}

impl std::fmt::Display for Command {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.command(self)?;
		Ok(())
	}
}
