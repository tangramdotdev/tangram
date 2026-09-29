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

	pub fn try_with_referent<T>(referent: tg::Referent<T>) -> std::result::Result<Self, T::Error>
	where
		T: TryInto<Id>,
	{
		let referent = referent.try_map(TryInto::try_into)?;

		Ok(Self::with_referent(referent))
	}

	#[must_use]
	pub fn with_referent(referent: tg::Referent<Id>) -> Self {
		let command = Self::with_id(referent.node);
		command.state().set_location(referent.options.location);
		command.state().set_tokens(referent.options.tokens);

		command
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
		let object = object.unwrap_command_ref().clone();
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

impl Command {
	pub async fn args(&self) -> tg::Result<impl Deref<Target = Vec<tg::command::Value>>> {
		let instance = tg::instance()?;
		self.args_with_instance(instance).await
	}

	pub async fn args_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = Vec<tg::command::Value>> + use<I>>
	where
		I: tg::Instance,
	{
		Ok(self
			.object_with_instance(instance)
			.await?
			.map(|object| &object.args))
	}

	pub async fn cwd(&self) -> tg::Result<impl Deref<Target = Option<PathBuf>>> {
		let instance = tg::instance()?;
		self.cwd_with_instance(instance).await
	}

	pub async fn cwd_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = Option<PathBuf>> + use<I>>
	where
		I: tg::Instance,
	{
		Ok(self
			.object_with_instance(instance)
			.await?
			.map(|object| &object.cwd))
	}

	pub async fn env(
		&self,
	) -> tg::Result<impl Deref<Target = BTreeMap<String, tg::command::Value>>> {
		let instance = tg::instance()?;
		self.env_with_instance(instance).await
	}

	pub async fn env_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = BTreeMap<String, tg::command::Value>>>
	where
		I: tg::Instance,
	{
		Ok(self
			.object_with_instance(instance)
			.await?
			.map(|object| &object.env))
	}

	pub async fn executable(&self) -> tg::Result<impl Deref<Target = tg::command::Executable>> {
		let instance = tg::instance()?;
		self.executable_with_instance(instance).await
	}

	pub async fn executable_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = tg::command::Executable>>
	where
		I: tg::Instance,
	{
		Ok(self
			.object_with_instance(instance)
			.await?
			.map(|object| &object.executable))
	}

	pub async fn host(&self) -> tg::Result<impl Deref<Target = String>> {
		let instance = tg::instance()?;
		self.host_with_instance(instance).await
	}

	pub async fn host_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = String>>
	where
		I: tg::Instance,
	{
		Ok(self
			.object_with_instance(instance)
			.await?
			.map(|object| &object.host))
	}

	pub async fn stdin(&self) -> tg::Result<impl Deref<Target = Option<tg::Blob>>> {
		let instance = tg::instance()?;
		self.stdin_with_instance(instance).await
	}

	pub async fn stdin_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = Option<tg::Blob>>>
	where
		I: tg::Instance,
	{
		Ok(self
			.object_with_instance(instance)
			.await?
			.map(|object| &object.stdin))
	}

	pub async fn user(&self) -> tg::Result<impl Deref<Target = Option<String>>> {
		let instance = tg::instance()?;
		self.user_with_instance(instance).await
	}

	pub async fn user_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = Option<String>>>
	where
		I: tg::Instance,
	{
		Ok(self
			.object_with_instance(instance)
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
