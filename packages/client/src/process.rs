use crate::{self as tg, handle::Ext as _, util::arc::Ext as _};
use std::{
	ops::Deref,
	sync::{Arc, Mutex, RwLock},
};

pub use self::{
	data::Data,
	id::Id,
	metadata::Metadata,
	mount::Mount,
	signal::Signal,
	state::State,
	status::Status,
	stdio::Stdio,
	wait::{Exit, Wait},
};

pub mod build;
pub mod children;
pub mod data;
pub mod dequeue;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod id;
pub mod log;
pub mod metadata;
pub mod mount;
pub mod put;
pub mod run;
pub mod signal;
pub mod spawn;
pub mod start;
pub mod state;
pub mod status;
pub mod stdio;
pub mod touch;
pub mod wait;

static CURRENT: Mutex<Option<tg::Process>> = Mutex::new(None);

#[derive(Clone, Debug)]
pub struct Process(Arc<Inner>);

#[derive(Debug)]
pub struct Inner {
	id: Id,
	remote: Option<String>,
	state: RwLock<Option<Arc<State>>>,
	metadata: RwLock<Option<Arc<Metadata>>>,
	token: Option<String>,
}

impl Process {
	#[must_use]
	pub fn new(
		id: Id,
		remote: Option<String>,
		state: Option<State>,
		metadata: Option<Metadata>,
		token: Option<String>,
	) -> Self {
		let state = RwLock::new(state.map(Arc::new));
		let metadata = RwLock::new(metadata.map(Arc::new));
		Self(Arc::new(Inner {
			id,
			remote,
			state,
			metadata,
			token,
		}))
	}

	pub fn current() -> tg::Result<Option<Self>> {
		if let Some(process) = CURRENT.lock().unwrap().as_ref() {
			return Ok(Some(process.clone()));
		}
		let Ok(id) = std::env::var("TANGRAM_PROCESS") else {
			return Ok(None);
		};
		let id = id.parse()?;
		let process = Self::new(id, None, None, None, None);
		CURRENT.lock().unwrap().replace(process.clone());
		Ok(Some(process))
	}

	#[must_use]
	pub fn id(&self) -> &Id {
		&self.id
	}

	#[must_use]
	pub fn remote(&self) -> Option<&String> {
		self.remote.as_ref()
	}

	#[must_use]
	pub fn state(&self) -> &RwLock<Option<Arc<State>>> {
		&self.state
	}

	#[must_use]
	pub fn metadata(&self) -> &RwLock<Option<Arc<Metadata>>> {
		&self.metadata
	}

	#[must_use]
	pub fn token(&self) -> Option<&String> {
		self.token.as_ref()
	}

	pub async fn load<H>(&self, handle: &H) -> tg::Result<Arc<tg::process::State>>
	where
		H: tg::Handle,
	{
		self.try_load(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the process"))
	}

	pub async fn try_load<H>(&self, handle: &H) -> tg::Result<Option<Arc<tg::process::State>>>
	where
		H: tg::Handle,
	{
		if let Some(state) = self.state.read().unwrap().clone() {
			return Ok(Some(state));
		}
		let Some(output) = handle.try_get_process(self.id()).await? else {
			return Ok(None);
		};
		let state = tg::process::State::try_from(output.data)?;
		let state = Arc::new(state);
		self.state.write().unwrap().replace(state.clone());
		Ok(Some(state))
	}

	pub async fn command<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = tg::Command> + use<H>>
	where
		H: tg::Handle,
	{
		Ok(self.load(handle).await?.map(|state| &state.command))
	}

	pub async fn retry<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = bool>>
	where
		H: tg::Handle,
	{
		Ok(self.load(handle).await?.map(|state| &state.retry))
	}

	pub async fn spawn<H>(handle: &H, arg: tg::process::spawn::Arg) -> tg::Result<tg::Process>
	where
		H: tg::Handle,
	{
		let output = handle.spawn_process(arg).await?;
		let process = tg::Process::new(output.process, output.remote, None, None, None);
		Ok(process)
	}

	pub async fn wait<H>(&self, handle: &H) -> tg::Result<tg::process::Wait>
	where
		H: tg::Handle,
	{
		handle.wait_process(&self.id).await?.try_into()
	}
}

impl Deref for Process {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
