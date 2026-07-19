use {
	crate::prelude::*,
	std::sync::{
		Arc, RwLock,
		atomic::{AtomicBool, Ordering},
	},
};

mod builder;
mod id;
mod isolation;
mod mount;
mod network;

pub use self::{
	builder::Builder,
	id::Id,
	isolation::Isolation,
	mount::Mount,
	network::{Bridge, Network, Port, Protocol as PortProtocol, Range as PortRange},
	status::Status,
};

pub mod control;
pub mod create;
pub mod destroy;
pub mod get;
pub mod list;
pub mod status;

#[derive(Clone, Debug)]
pub struct Sandbox(Arc<Inner>);

#[derive(derive_more::Debug)]
struct Inner {
	#[debug(ignore)]
	handle: Option<tg::handle::dynamic::Handle>,
	id: Id,
	location: RwLock<Option<tg::location::Arg>>,
	owned: AtomicBool,
	state: RwLock<Option<Arc<tg::sandbox::get::Output>>>,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
	pub state: Option<tg::sandbox::get::Output>,
}

impl Sandbox {
	#[must_use]
	pub fn builder() -> tg::sandbox::Builder {
		tg::sandbox::Builder::new()
	}

	#[must_use]
	pub fn with_id(id: Id) -> Self {
		Self::new(id, tg::sandbox::Options::default())
	}

	#[must_use]
	pub fn new(id: Id, options: tg::sandbox::Options) -> Self {
		Self::new_inner(id, options, None)
	}

	#[must_use]
	fn new_inner(
		id: Id,
		options: tg::sandbox::Options,
		handle: Option<tg::handle::dynamic::Handle>,
	) -> Self {
		let location = RwLock::new(options.location);
		let owned = AtomicBool::new(handle.is_some());
		let state = RwLock::new(options.state.map(Arc::new));
		let inner = Inner {
			handle,
			id,
			location,
			owned,
			state,
		};

		Self(Arc::new(inner))
	}

	#[must_use]
	pub fn id(&self) -> &Id {
		&self.0.id
	}

	#[must_use]
	pub fn location(&self) -> Option<tg::location::Arg> {
		self.0.location.read().unwrap().clone()
	}

	#[must_use]
	pub fn state(&self) -> &RwLock<Option<Arc<tg::sandbox::get::Output>>> {
		&self.0.state
	}

	pub async fn destroy(&self) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.destroy_with_handle(handle).await
	}

	pub async fn destroy_with_handle<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let arg = tg::sandbox::destroy::Arg {
			error: None,
			location: self.location(),
		};
		handle.destroy_sandbox(self.id(), arg).await?;
		self.detach();

		Ok(())
	}

	pub fn detach(&self) {
		self.0.owned.store(false, Ordering::SeqCst);
	}

	pub async fn load(&self) -> tg::Result<Arc<tg::sandbox::get::Output>> {
		let handle = tg::handle()?;
		self.load_with_handle(handle).await
	}

	pub async fn load_with_handle<H>(&self, handle: &H) -> tg::Result<Arc<tg::sandbox::get::Output>>
	where
		H: tg::Handle,
	{
		self.try_load_with_handle(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the sandbox"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<tg::sandbox::get::Output>>> {
		let handle = tg::handle()?;
		self.try_load_with_handle(handle).await
	}

	pub async fn try_load_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<Option<Arc<tg::sandbox::get::Output>>>
	where
		H: tg::Handle,
	{
		if let Some(state) = self.0.state.read().unwrap().clone() {
			return Ok(Some(state));
		}
		let arg = tg::sandbox::get::Arg {
			location: self.location(),
		};
		let Some(output) = handle.try_get_sandbox(self.id(), arg).await? else {
			return Ok(None);
		};
		if let Some(location) = &output.location {
			self.0
				.location
				.write()
				.unwrap()
				.replace(location.clone().into());
		}
		let state = Arc::new(output);
		self.0.state.write().unwrap().replace(state.clone());

		Ok(Some(state))
	}

	pub async fn create() -> tg::Result<Self> {
		Self::builder().build().await
	}

	pub async fn create_with_handle<H>(handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		Self::builder().build_with_handle(handle).await
	}

	pub async fn create_with_arg(arg: tg::sandbox::create::Arg) -> tg::Result<Self> {
		let handle = tg::handle()?;
		Self::create_with_arg_with_handle(handle, arg).await
	}

	pub async fn create_with_arg_with_handle<H>(
		handle: &H,
		mut arg: tg::sandbox::create::Arg,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		arg.host
			.get_or_insert_with(|| tg::host::current().to_owned());
		let location = arg.location.clone();
		let output = handle.create_sandbox(arg).await?;
		let handle = tg::handle::dynamic::Handle::new(handle.clone());
		let options = tg::sandbox::Options {
			location,
			state: None,
		};
		let sandbox = Self::new_inner(output.id, options, Some(handle));

		Ok(sandbox)
	}

	pub async fn run(&self, arg: tg::process::Arg) -> tg::Result<tg::Value> {
		let handle = tg::handle()?;
		self.run_with_handle(handle, arg).await
	}

	pub async fn run_with_handle<H>(
		&self,
		handle: &H,
		mut arg: tg::process::Arg,
	) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		arg.location = self.location().or(arg.location);
		arg.sandbox = Some(tg::process::SandboxArg::Id(self.id().clone()));

		tg::Process::<tg::Value>::run_with_handle(handle, arg).await
	}
}

impl From<tg::sandbox::Id> for Sandbox {
	fn from(value: tg::sandbox::Id) -> Self {
		Self::with_id(value)
	}
}

impl Drop for Inner {
	fn drop(&mut self) {
		if !self.owned.swap(false, Ordering::SeqCst) {
			return;
		}
		let Some(handle) = self.handle.take() else {
			return;
		};
		let id = self.id.clone();
		let location = self.location.read().unwrap().clone();
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		runtime.spawn(async move {
			let arg = tg::sandbox::destroy::Arg {
				error: None,
				location,
			};
			handle.try_destroy_sandbox(&id, arg).await.ok();
		});
	}
}
