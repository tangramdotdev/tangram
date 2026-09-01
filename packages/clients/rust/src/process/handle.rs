use {
	super::{Id, State, Wait},
	crate::prelude::*,
	std::{
		marker::PhantomData,
		ops::Deref,
		sync::{
			Arc, Mutex, RwLock,
			atomic::{AtomicBool, Ordering},
		},
	},
	tangram_util::arc::Ext as _,
};

#[derive(Clone, Debug)]
pub struct Process<O = tg::Value>(pub(super) Arc<Inner>, pub(super) PhantomData<fn() -> O>);

#[derive(derive_more::Debug)]
pub(super) struct Inner {
	pub(super) cached: Option<bool>,
	#[debug(ignore)]
	pub(super) handle: Option<tg::handle::dynamic::Handle>,
	pub(super) id: tg::Either<u32, Id>,
	pub(super) lease: Option<String>,
	pub(super) location: Arc<RwLock<Option<tg::location::Arg>>>,
	pub(super) owned: AtomicBool,
	pub(super) state: RwLock<Option<Arc<State>>>,
	pub(super) stderr: tg::process::stdio::Reader,
	pub(super) stdin: tg::process::stdio::Writer,
	#[debug(ignore)]
	pub(super) stdio_task: Option<tangram_futures::task::Shared<tg::Result<()>>>,
	pub(super) stdout: tg::process::stdio::Reader,
	#[debug(ignore)]
	pub(super) task: Option<tangram_futures::task::Shared<tg::Result<tg::process::wait::Output>>>,
	pub(super) tokens: RwLock<tg::authorization::Tokens>,
	pub(super) wait: Mutex<Option<Wait>>,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub cached: Option<bool>,
	pub lease: Option<String>,
	pub location: Option<tg::location::Arg>,
	pub state: Option<State>,
	pub tokens: tg::authorization::Tokens,
}

impl<O> Process<O> {
	pub fn try_with_referent<T>(referent: tg::Referent<T>) -> std::result::Result<Self, T::Error>
	where
		T: TryInto<Id>,
	{
		let referent = referent.try_map(TryInto::try_into)?;

		Ok(Self::with_referent(referent))
	}

	#[must_use]
	pub fn with_referent(referent: tg::Referent<Id>) -> Self {
		let options = tg::process::Options {
			location: referent.options.location.map(Into::into),
			tokens: referent.options.tokens,
			..Default::default()
		};

		Self::new(referent.node, options)
	}

	#[must_use]
	pub fn new(id: Id, options: tg::process::Options) -> Self {
		let tg::process::Options {
			cached,
			lease,
			location,
			state,
			tokens,
		} = options;
		let location = Arc::new(RwLock::new(location));
		let state = RwLock::new(state.map(Arc::new));
		let stderr = tg::process::stdio::Reader::from_process(tg::process::stdio::Stream::Stderr);
		let stdin = tg::process::stdio::Writer::from_process(tg::process::stdio::Stream::Stdin);
		let stdout = tg::process::stdio::Reader::from_process(tg::process::stdio::Stream::Stdout);
		let inner = Arc::new(Inner {
			cached,
			handle: None,
			id: tg::Either::Right(id),
			lease,
			location: location.clone(),
			owned: AtomicBool::new(false),
			state,
			stderr,
			stdin,
			stdio_task: None,
			stdout,
			task: None,
			tokens: RwLock::new(tokens),
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
		self.0.cached
	}

	#[must_use]
	pub fn id(&self) -> tg::Either<&u32, &Id> {
		self.0.id.as_ref()
	}

	#[must_use]
	pub fn location(&self) -> Option<tg::location::Arg> {
		self.0.location.read().unwrap().clone()
	}

	#[must_use]
	pub fn state(&self) -> &RwLock<Option<Arc<State>>> {
		&self.0.state
	}

	#[must_use]
	pub fn tokens(&self) -> tg::authorization::Tokens {
		self.0.tokens.read().unwrap().clone()
	}

	#[must_use]
	pub fn wait_output(&self) -> Option<tg::process::wait::Output> {
		self.0.wait.lock().unwrap().as_ref().map(Wait::to_data)
	}

	pub(crate) fn inherit_location(&self, location: Option<tg::location::Arg>) {
		if self.location().is_none() {
			*self.0.location.write().unwrap() = location;
		}
	}

	pub(crate) fn inherit_tokens(&self, tokens: &tg::authorization::Tokens) {
		self.0.tokens.write().unwrap().inherit(tokens);
	}

	#[must_use]
	pub fn unwrap_state(&self) -> Arc<State> {
		self.0
			.state
			.read()
			.unwrap()
			.as_ref()
			.expect("process state should be loaded")
			.clone()
	}

	#[must_use]
	pub fn lease(&self) -> Option<&String> {
		self.0.lease.as_ref()
	}

	pub fn detach(&self) {
		self.0.owned.store(false, Ordering::SeqCst);
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
		if let Some(state) = self.0.state.read().unwrap().clone() {
			let location = self.location().and_then(|location| location.to_location());
			state.inherit_location(location.as_ref());
			let tokens = self.tokens();
			state.inherit_tokens(&tokens);
			return Ok(Some(state));
		}
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"loading unsandboxed process state is not supported"
			));
		};
		let arg = tg::process::get::Arg {
			availability: false,
			location: self.location(),
			metadata: false,
			tokens: self.tokens(),
		};
		let Some(output) = handle.try_get_process(id, arg).await? else {
			return Ok(None);
		};
		if !output.tokens.is_empty() {
			*self.0.tokens.write().unwrap() = output.tokens;
		}
		let location = output.location;
		if let Some(location) = &location {
			self.0
				.location
				.write()
				.unwrap()
				.replace(location.clone().into());
		}
		let state = tg::process::State::try_from(output.data)?;
		state.inherit_location(location.as_ref());
		let tokens = self.tokens();
		state.inherit_tokens(&tokens);
		let state = Arc::new(state);
		self.0.state.write().unwrap().replace(state.clone());
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

	pub async fn signal(
		&self,
		signal: tg::process::Signal,
		options: tg::process::signal::Options,
	) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.signal_with_handle(handle, signal, options).await
	}

	pub async fn signal_with_handle<H>(
		&self,
		handle: &H,
		signal: tg::process::Signal,
		options: tg::process::signal::Options,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if let Some(pid) = self.id().left() {
			let pid = i32::try_from(*pid)
				.map_err(|error| tg::error!(!error, "failed to convert the process id"))?;
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

		if options.location.is_none() && self.location().is_none() {
			self.ensure_location_with_handle(handle).await?;
		}
		let arg = tg::process::signal::post::Arg {
			location: options.location.or_else(|| self.location()),
			signal,
			tokens: self.tokens(),
		};
		let id = self.id().unwrap_right();
		handle.signal_process(id, arg).await?;

		Ok(())
	}

	pub async fn output(&self, options: tg::process::wait::Options) -> tg::Result<O>
	where
		O: TryFrom<tg::Value>,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let handle = tg::handle()?;
		self.output_with_handle(handle, options).await
	}

	pub async fn output_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::wait::Options,
	) -> tg::Result<O>
	where
		H: tg::Handle,
		O: TryFrom<tg::Value>,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let wait = self.wait_with_handle(handle, options).await?;
		let output = wait.into_output()?;
		let tokens = self.tokens();
		output.inherit_tokens(&tokens);
		output
			.try_into()
			.map_err(|error| tg::error!(source = error, "failed to convert the process output"))
	}
}

impl Drop for Inner {
	fn drop(&mut self) {
		let owned = self.owned.swap(false, Ordering::SeqCst);
		if self.id.is_left() {
			if !owned && let Some(task) = &mut self.task {
				task.detach();
			}
			return;
		}
		if !owned {
			return;
		}
		let Some(handle) = self.handle.take() else {
			return;
		};
		let Some(lease) = self.lease.clone() else {
			return;
		};
		let id = self.id.as_ref().unwrap_right().clone();
		let location = self.location.read().unwrap().clone();
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		runtime.spawn(async move {
			let arg = tg::process::cancel::Arg { location, lease };
			handle.try_cancel_process(&id, arg).await.ok();
		});
	}
}
