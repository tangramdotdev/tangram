use {
	crate::prelude::*,
	std::sync::{
		Arc, RwLock,
		atomic::{AtomicBool, Ordering},
	},
};

mod builder;
mod data;
mod id;
mod isolation;
mod mount;
mod network;

pub use self::{
	builder::Builder,
	data::{Data, Usage},
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
pub mod processes;
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
	tokens: RwLock<tg::authorization::Tokens>,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
	pub state: Option<tg::sandbox::get::Output>,
	pub tokens: tg::authorization::Tokens,
}

impl Sandbox {
	#[must_use]
	pub fn builder() -> tg::sandbox::Builder {
		tg::sandbox::Builder::new()
	}

	#[must_use]
	pub fn with_referent(referent: tg::Referent<Id>) -> Self {
		let options = tg::sandbox::Options {
			location: referent.options.location.map(Into::into),
			tokens: referent.options.tokens,
			..tg::sandbox::Options::default()
		};

		Self::new(referent.node, options)
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
		let tg::sandbox::Options {
			location,
			state,
			tokens,
		} = options;
		let location = RwLock::new(location);
		let owned = AtomicBool::new(handle.is_some());
		let mut tokens = tokens;
		if let Some(state) = &state {
			tokens.inherit(&state.tokens);
		}
		let state = RwLock::new(state.map(Arc::new));
		let tokens = RwLock::new(tokens);
		let inner = Inner {
			handle,
			id,
			location,
			owned,
			state,
			tokens,
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

	#[must_use]
	pub fn tokens(&self) -> tg::authorization::Tokens {
		self.0.tokens.read().unwrap().clone()
	}

	pub fn detach(&self) {
		self.0.owned.store(false, Ordering::SeqCst);
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
