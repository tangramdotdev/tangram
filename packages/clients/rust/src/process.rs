use {
	crate::prelude::*,
	std::{
		ops::Deref,
		path::PathBuf,
		sync::{Arc, Mutex, RwLock},
	},
	tangram_util::arc::Ext as _,
};

pub use self::{
	build::build, data::Data, id::Id, metadata::Metadata, run::run, signal::Signal, state::State,
	status::Status, stdio::Stdio, tty::Tty, wait::Wait,
};

pub mod build;
pub mod cancel;
pub mod children;
pub mod data;
pub mod finish;
pub mod get;
pub mod id;
pub mod list;
pub mod metadata;
pub mod put;
pub mod queue;
pub mod run;
pub mod signal;
pub mod spawn;
pub mod state;
pub mod status;
pub mod stdio;
pub mod touch;
pub mod tty;
pub mod wait;

#[derive(Clone, Debug)]
pub struct Process(Arc<Inner>);

#[derive(derive_more::Debug)]
pub struct Inner {
	cached: Option<bool>,
	id: Id,
	metadata: RwLock<Option<Arc<Metadata>>>,
	pid: Option<u32>,
	remote: Option<String>,
	state: RwLock<Option<Arc<State>>>,
	stderr: tg::process::stdio::Reader,
	stdin: tg::process::stdio::Writer,
	#[debug(ignore)]
	stdio_task: Option<tangram_futures::task::Shared<tg::Result<()>>>,
	stdout: tg::process::stdio::Reader,
	#[debug(ignore)]
	task: Option<tangram_futures::task::Shared<tg::Result<tg::process::wait::Output>>>,
	token: Option<String>,
	wait: Mutex<Option<Wait>>,
}

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub args: tg::value::Array,
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub cwd: Option<PathBuf>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: Option<String>,
	pub name: Option<String>,
	pub parent: Option<tg::process::Id>,
	pub progress: bool,
	pub remote: Option<String>,
	pub retry: bool,
	pub sandbox: Option<tg::Either<tg::sandbox::create::Arg, tg::sandbox::Id>>,
	pub stderr: tg::process::Stdio,
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub tty: Option<tg::Either<bool, tg::process::Tty>>,
	pub user: Option<String>,
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
		Self::new_with_cached(id, metadata, remote, state, token, None)
	}

	#[must_use]
	pub(crate) fn new_with_cached(
		id: Id,
		metadata: Option<Metadata>,
		remote: Option<String>,
		state: Option<State>,
		token: Option<String>,
		cached: Option<bool>,
	) -> Self {
		let metadata = RwLock::new(metadata.map(Arc::new));
		let state = RwLock::new(state.map(Arc::new));
		let stderr = tg::process::stdio::Reader::from_process(
			id.clone(),
			remote.clone(),
			tg::process::stdio::Stream::Stderr,
		);
		let stdin = tg::process::stdio::Writer::from_process(
			id.clone(),
			remote.clone(),
			tg::process::stdio::Stream::Stdin,
		);
		let stdout = tg::process::stdio::Reader::from_process(
			id.clone(),
			remote.clone(),
			tg::process::stdio::Stream::Stdout,
		);
		Self(Arc::new(Inner {
			cached,
			id,
			metadata,
			pid: None,
			remote,
			state,
			stderr,
			stdin,
			stdio_task: None,
			stdout,
			task: None,
			token,
			wait: Mutex::new(None),
		}))
	}

	#[must_use]
	pub fn cached(&self) -> Option<bool> {
		self.cached
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

	#[must_use]
	pub fn stdin(&self) -> tg::process::stdio::Writer {
		self.0.stdin.clone()
	}

	#[must_use]
	pub fn stdout(&self) -> tg::process::stdio::Reader {
		self.0.stdout.clone()
	}

	#[must_use]
	pub fn stderr(&self) -> tg::process::stdio::Reader {
		self.0.stderr.clone()
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

	pub async fn signal<H>(&self, handle: &H, signal: tg::process::Signal) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if let Some(pid) = self.pid {
			let pid = i32::try_from(pid)
				.map_err(|source| tg::error!(!source, "failed to convert the process id"))?;
			let signal = i32::from(signal as u8);
			let ret = unsafe { libc::kill(pid, signal) };
			if ret < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to signal the process"
				));
			}
			return Ok(());
		}

		let arg = tg::process::signal::post::Arg {
			local: self.remote.is_none().then_some(true),
			remotes: self.remote.clone().map(|remote| vec![remote]),
			signal,
		};
		handle.signal_process(self.id(), arg).await?;

		Ok(())
	}

	pub async fn wait<H>(
		&self,
		handle: &H,
		arg: tg::process::wait::Arg,
	) -> tg::Result<tg::process::Wait>
	where
		H: tg::Handle,
	{
		if let Some(task) = self.stdio_task.as_ref() {
			task.wait()
				.await
				.map_err(|source| tg::error!(!source, "the stdio task panicked"))??;
		}
		if let Some(task) = &self.task {
			let output = task
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "the task panicked"))??;
			return output.try_into();
		}
		if let Some(wait) = self.wait.lock().unwrap().take() {
			return Ok(wait);
		}
		let wait = handle.wait_process(&self.id, arg).await?.try_into()?;
		Ok(wait)
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
