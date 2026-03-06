use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _},
	std::{
		future::Future,
		ops::Deref,
		pin::Pin,
		sync::{Arc, Mutex, RwLock},
	},
	tangram_futures::stream::TryExt as _,
	tangram_util::arc::Ext as _,
};

pub use self::{
	data::Data, id::Id, metadata::Metadata, mount::Mount, pty::Pty, signal::Signal, state::State,
	status::Status, stdio::Stdio, wait::Wait,
};

pub mod cancel;
pub mod children;
pub mod data;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod id;
pub mod list;
pub mod log;
pub mod metadata;
pub mod mount;
pub mod pty;
pub mod put;
pub mod queue;
pub mod signal;
pub mod spawn;
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
	metadata: RwLock<Option<Arc<Metadata>>>,
	remote: Option<String>,
	state: RwLock<Option<Arc<State>>>,
	token: Option<String>,
	wait: Mutex<Option<Wait>>,
}

impl Process {
	#[must_use]
	pub fn new(
		id: Id,
		metadata: Option<Metadata>,
		remote: Option<String>,
		state: Option<State>,
		token: Option<String>,
	) -> Self {
		let metadata = RwLock::new(metadata.map(Arc::new));
		let state = RwLock::new(state.map(Arc::new));
		Self(Arc::new(Inner {
			id,
			metadata,
			remote,
			state,
			token,
			wait: Mutex::new(None),
		}))
	}

	pub fn current() -> tg::Result<Option<Self>> {
		if let Some(process) = CURRENT.lock().unwrap().as_ref() {
			return Ok(Some(process.clone()));
		}
		let Ok(id) = std::env::var("TANGRAM_PROCESS") else {
			return Ok(None);
		};
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
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
	pub fn unwrap_state(&self) -> Arc<State> {
		self.state
			.read()
			.unwrap()
			.as_ref()
			.expect("process state should be loaded")
			.clone()
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
		let arg = tg::process::get::Arg::default();
		let Some(output) = handle.try_get_process(self.id(), arg).await? else {
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
		Self::spawn_with_progress(handle, arg, |stream| async move {
			stream
				.try_last()
				.await?
				.and_then(|event| event.try_unwrap_output().ok())
				.ok_or_else(|| tg::error!("expected the stream to contain an output event"))
		})
		.await
	}

	pub async fn spawn_with_progress<H, F, Fut>(
		handle: &H,
		arg: tg::process::spawn::Arg,
		progress: F,
	) -> tg::Result<tg::Process>
	where
		H: tg::Handle,
		F: FnOnce(
			Pin<Box<dyn Stream<Item = tg::Result<tg::progress::Event<tg::Process>>> + Send>>,
		) -> Fut,
		Fut: Future<Output = tg::Result<tg::Process>>,
	{
		let stream = handle
			.spawn_process(arg)
			.await?
			.and_then(|event| async move {
				let event = match event {
					tg::progress::Event::Output(output) => {
						let wait = output
							.wait
							.map(tg::process::Wait::try_from_data)
							.transpose()?;
						let process = tg::Process::new(
							output.process,
							None,
							output.remote,
							None,
							output.token,
						);
						if let Some(wait) = wait {
							process.wait.lock().unwrap().replace(wait);
						}
						tg::progress::Event::Output(process)
					},
					tg::progress::Event::Log(log) => tg::progress::Event::Log(log),
					tg::progress::Event::Diagnostic(diagnostic) => {
						tg::progress::Event::Diagnostic(diagnostic)
					},
					tg::progress::Event::Indicators(indicators) => {
						tg::progress::Event::Indicators(indicators)
					},
				};
				Ok(event)
			});
		progress(Box::pin(stream)).await
	}

	pub async fn wait<H>(
		&self,
		handle: &H,
		arg: tg::process::wait::Arg,
	) -> tg::Result<tg::process::Wait>
	where
		H: tg::Handle,
	{
		if let Some(wait) = self.wait.lock().unwrap().take() {
			return Ok(wait);
		}
		handle.wait_process(&self.id, arg).await?.try_into()
	}

	pub async fn output<H>(&self, handle: &H) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let wait = self.wait(handle, tg::process::wait::Arg::default()).await?;
		let output = wait.into_output()?;
		Ok(output)
	}
}

impl Deref for Process {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
