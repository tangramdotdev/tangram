use {
	crate::prelude::*,
	std::{
		marker::PhantomData,
		ops::Deref,
		path::PathBuf,
		sync::{Arc, Mutex, RwLock},
	},
	tangram_util::arc::Ext as _,
};

pub use self::{
	build::{build, build_with_handle},
	data::Data,
	debug::Debug,
	env::env,
	exec::{exec, exec_with_handle},
	id::Id,
	metadata::Metadata,
	run::{run, run_with_handle},
	signal::Signal,
	spawn::{spawn, spawn_with_handle},
	state::State,
	status::Status,
	stdio::Stdio,
	tty::Tty,
	wait::Wait,
};

pub mod build;
pub mod cancel;
pub mod children;
pub mod data;
pub mod debug;
pub mod env;
pub mod exec;
pub mod finish;
pub mod get;
pub mod id;
pub mod list;
pub mod metadata;
pub mod put;
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
pub struct Process<O = tg::Value>(Arc<Inner>, PhantomData<fn() -> O>);

#[derive(derive_more::Debug)]
pub struct Inner {
	cached: Option<bool>,
	id: tg::Either<u32, Id>,
	location: Arc<RwLock<Option<tg::location::Arg>>>,
	metadata: RwLock<Option<Arc<Metadata>>>,
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
	pub cpu: Option<u64>,
	pub cwd: Option<PathBuf>,
	pub debug: Option<tg::process::Debug>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: Option<String>,
	pub location: Option<tg::location::Arg>,
	pub memory: Option<u64>,
	pub name: Option<String>,
	pub parent: Option<tg::process::Id>,
	pub progress: bool,
	pub retry: bool,
	pub sandbox: Option<tg::Either<tg::sandbox::create::Arg, tg::sandbox::Id>>,
	pub stderr: tg::process::Stdio,
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub tty: Option<tg::Either<bool, tg::process::Tty>>,
	pub user: Option<String>,
}

impl<O> Process<O> {
	#[must_use]
	pub fn new(
		id: Id,
		location: Option<tg::location::Arg>,
		metadata: Option<Metadata>,
		state: Option<State>,
		token: Option<String>,
		cached: Option<bool>,
	) -> Self {
		let location = Arc::new(RwLock::new(location));
		let metadata = RwLock::new(metadata.map(Arc::new));
		let state = RwLock::new(state.map(Arc::new));
		let stderr = tg::process::stdio::Reader::from_process(tg::process::stdio::Stream::Stderr);
		let stdin = tg::process::stdio::Writer::from_process(tg::process::stdio::Stream::Stdin);
		let stdout = tg::process::stdio::Reader::from_process(tg::process::stdio::Stream::Stdout);
		let inner = Arc::new(Inner {
			cached,
			id: tg::Either::Right(id),
			location: location.clone(),
			metadata,
			state,
			stderr,
			stdin,
			stdio_task: None,
			stdout,
			task: None,
			token,
			wait: Mutex::new(None),
		});
		let process = Self(inner, PhantomData);
		process.stdin().set_process(Arc::downgrade(&process.0));
		process.stdout().set_process(Arc::downgrade(&process.0));
		process.stderr().set_process(Arc::downgrade(&process.0));
		process
	}

	#[must_use]
	pub fn cached(&self) -> Option<bool> {
		self.cached
	}

	#[must_use]
	pub fn id(&self) -> tg::Either<&u32, &Id> {
		self.0.id.as_ref()
	}

	#[must_use]
	pub fn location(&self) -> Option<tg::location::Arg> {
		self.location.read().unwrap().clone()
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

	pub(crate) async fn ensure_location_with_handle<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if self.id().is_left() || self.location().is_some() {
			return Ok(());
		}
		self.try_load_with_handle(handle).await?;
		Ok(())
	}

	pub async fn load(&self) -> tg::Result<Arc<tg::process::State>> {
		let handle = tg::handle()?;
		self.load_with_handle(handle).await
	}

	pub async fn load_with_handle<H>(&self, handle: &H) -> tg::Result<Arc<tg::process::State>>
	where
		H: tg::Handle,
	{
		self.try_load_with_handle(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to load the process"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<tg::process::State>>> {
		let handle = tg::handle()?;
		self.try_load_with_handle(handle).await
	}

	pub async fn try_load_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<Option<Arc<tg::process::State>>>
	where
		H: tg::Handle,
	{
		if let Some(state) = self.state.read().unwrap().clone() {
			return Ok(Some(state));
		}
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"loading unsandboxed process state is not supported"
			));
		};
		let arg = tg::process::get::Arg {
			location: self.location(),
			metadata: false,
		};
		let Some(output) = handle.try_get_process(id, arg).await? else {
			return Ok(None);
		};
		if let Some(location) = output.location {
			self.location.write().unwrap().replace(location.into());
		}
		let state = tg::process::State::try_from(output.data)?;
		let state = Arc::new(state);
		self.state.write().unwrap().replace(state.clone());
		Ok(Some(state))
	}

	pub async fn command(&self) -> tg::Result<impl Deref<Target = tg::Command>> {
		let handle = tg::handle()?;
		self.command_with_handle(handle).await
	}

	pub async fn command_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<impl Deref<Target = tg::Command> + use<H, O>>
	where
		H: tg::Handle,
	{
		Ok(self
			.load_with_handle(handle)
			.await?
			.map(|state| &state.command))
	}

	pub async fn retry(&self) -> tg::Result<impl Deref<Target = bool>> {
		let handle = tg::handle()?;
		self.retry_with_handle(handle).await
	}

	pub async fn retry_with_handle<H>(&self, handle: &H) -> tg::Result<impl Deref<Target = bool>>
	where
		H: tg::Handle,
	{
		Ok(self
			.load_with_handle(handle)
			.await?
			.map(|state| &state.retry))
	}

	pub async fn signal(&self, signal: tg::process::Signal) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.signal_with_handle(handle, signal).await
	}

	pub async fn signal_with_handle<H>(
		&self,
		handle: &H,
		signal: tg::process::Signal,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if let Some(pid) = self.id().left() {
			let pid = i32::try_from(*pid)
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

		self.ensure_location_with_handle(handle).await?;
		let arg = tg::process::signal::post::Arg {
			signal,
			location: self.location(),
		};
		let id = self.id().unwrap_right();
		handle.signal_process(id, arg).await?;

		Ok(())
	}

	pub async fn wait(&self, arg: tg::process::wait::Arg) -> tg::Result<tg::process::Wait> {
		let handle = tg::handle()?;
		self.wait_with_handle(handle, arg).await
	}

	pub async fn wait_with_handle<H>(
		&self,
		handle: &H,
		mut arg: tg::process::wait::Arg,
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
		if arg.token.is_none() {
			arg.token = self.token().cloned();
		}
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"waiting for an unsandboxed process is not supported"
			));
		};
		let wait = handle.wait_process(id, arg).await?.try_into()?;
		Ok(wait)
	}

	pub async fn output(&self) -> tg::Result<O>
	where
		O: TryFrom<tg::Value>,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let handle = tg::handle()?;
		self.output_with_handle(handle).await
	}

	pub async fn output_with_handle<H>(&self, handle: &H) -> tg::Result<O>
	where
		H: tg::Handle,
		O: TryFrom<tg::Value>,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let arg = tg::process::wait::Arg {
			location: self.location(),
			token: self.token().cloned(),
		};
		let wait = self.wait_with_handle(handle, arg).await?;
		let output = wait.into_output()?;
		output
			.try_into()
			.map_err(|source| tg::error!(source = source, "failed to convert the process output"))
	}
}

impl<O> Deref for Process<O> {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub(crate) fn normalize_sandbox_arg(
	sandbox: Option<tg::Either<tg::sandbox::create::Arg, tg::sandbox::Id>>,
	cpu: Option<u64>,
	memory: Option<u64>,
) -> tg::Result<Option<tg::Either<tg::sandbox::create::Arg, tg::sandbox::Id>>> {
	let has_resource_fields = cpu.is_some() || memory.is_some();
	match sandbox {
		Some(tg::Either::Left(mut sandbox)) => {
			if let Some(cpu) = cpu {
				sandbox.cpu = Some(cpu);
			}
			if let Some(memory) = memory {
				sandbox.memory = Some(memory);
			}
			Ok(Some(tg::Either::Left(sandbox)))
		},
		Some(tg::Either::Right(sandbox)) => {
			if has_resource_fields {
				return Err(tg::error!(
					"cpu and memory are not supported for existing sandboxes"
				));
			}
			Ok(Some(tg::Either::Right(sandbox)))
		},
		None => {
			if !has_resource_fields {
				return Ok(None);
			}
			Ok(Some(tg::Either::Left(tg::sandbox::create::Arg {
				cpu,
				memory,
				network: false,
				ttl: 0,
				..Default::default()
			})))
		},
	}
}
