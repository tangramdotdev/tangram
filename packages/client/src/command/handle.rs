use super::{Builder, Data, Id, Object};
use crate::{self as tg, util::arc::Ext as _};
use futures::{
	TryStreamExt as _,
	stream::{FuturesOrdered, FuturesUnordered},
};
use std::{collections::BTreeMap, ops::Deref, path::PathBuf, sync::Arc};

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
	pub fn state(&self) -> &std::sync::RwLock<State> {
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

	pub async fn id<H>(&self, handle: &H) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		self.store(handle).await
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
		let Some(output) = handle.try_get_object(&id.into()).await? else {
			return Ok(None);
		};
		let data = Data::deserialize(output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let object = Object::try_from(data)?;
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
		if let Some(id) = self.state.read().unwrap().id.clone() {
			return Ok(id);
		}
		let data = self.data(handle).await?;
		let bytes = data.serialize()?;
		let id = Id::new(&bytes);
		let arg = tg::object::put::Arg { bytes };
		handle
			.put_object(&id.clone().into(), arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		self.state.write().unwrap().id.replace(id.clone());
		Ok(id)
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
		let object = self.object(handle).await?;
		let args = object
			.args
			.iter()
			.map(|value| value.data(handle))
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		let cwd = object.cwd.clone();
		let env = object
			.env
			.iter()
			.map(|(key, value)| async move {
				let key = key.clone();
				let value = value.data(handle).await?;
				Ok::<_, tg::Error>((key, value))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		let executable = match &object.executable {
			tg::command::Executable::Artifact(executable) => {
				let artifact = tg::command::data::Artifact {
					artifact: executable.artifact.id(handle).await?,
					subpath: executable.subpath.clone(),
				};
				tg::command::data::Executable::Artifact(artifact)
			},
			tg::command::Executable::Module(executable) => {
				let module = Box::pin(executable.data(handle)).await?;
				tg::command::data::Executable::Module(module)
			},
			tg::command::Executable::Path(executable) => {
				let path = tg::command::data::Path {
					path: executable.path.clone(),
				};
				tg::command::data::Executable::Path(path)
			},
		};
		let host = object.host.clone();
		let mounts = if let Some(mounts) = &object.mounts {
			let data = mounts
				.iter()
				.map(|mount| mount.data(handle))
				.collect::<FuturesOrdered<_>>()
				.try_collect()
				.await?;
			Some(data)
		} else {
			None
		};
		let stdin = if let Some(stdin) = &object.stdin {
			Some(stdin.id(handle).await?)
		} else {
			None
		};
		let user = object.user.clone();
		Ok(Data {
			args,
			cwd,
			env,
			executable,
			host,
			mounts,
			stdin,
			user,
		})
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
	) -> tg::Result<impl Deref<Target = Option<Vec<tg::command::Mount>>>>
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
}

impl std::fmt::Display for Command {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.command(self)?;
		Ok(())
	}
}
