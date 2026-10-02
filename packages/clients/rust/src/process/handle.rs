use {
	super::{Id, Outcome, State},
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
	pub(super) connection: Option<tg::process::connect::Connection>,
	pub(super) id: tg::Either<u32, Id>,
	#[debug(ignore)]
	pub(super) instance: Option<tg::instance::dynamic::Instance>,
	pub(super) lease: Option<String>,
	pub(super) location: Arc<RwLock<Option<tg::location::Arg>>>,
	pub(super) outcome: Mutex<Option<Outcome>>,
	pub(super) owned: AtomicBool,
	pub(super) state: RwLock<Option<Arc<State>>>,
	pub(super) stderr: tg::process::stdio::Reader,
	pub(super) stdin: tg::process::stdio::Writer,
	#[debug(ignore)]
	pub(super) stdio_task: Option<tangram_futures::task::Shared<tg::Result<()>>>,
	pub(super) stdout: tg::process::stdio::Reader,
	#[debug(ignore)]
	pub(super) task: Option<tangram_futures::task::Shared<tg::Result<tg::process::outcome::Data>>>,
	pub(super) tokens: RwLock<tg::authorization::Tokens>,
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
		Self::new_inner(id, options, None, None)
	}

	#[must_use]
	pub(super) fn new_inner(
		id: Id,
		options: tg::process::Options,
		instance: Option<tg::instance::dynamic::Instance>,
		connection: Option<tg::process::connect::Connection>,
	) -> Self {
		let tg::process::Options {
			cached,
			lease,
			location,
			state,
			tokens,
		} = options;
		let mut tokens = tokens;
		tokens.normalize(None);
		let location = Arc::new(RwLock::new(location));
		let state = RwLock::new(state.map(Arc::new));
		let stderr = tg::process::stdio::Reader::from_process(tg::process::stdio::Stream::Stderr);
		let stdin = tg::process::stdio::Writer::from_process(tg::process::stdio::Stream::Stdin);
		let stdout = tg::process::stdio::Reader::from_process(tg::process::stdio::Stream::Stdout);
		let owned = AtomicBool::new(instance.is_some() && lease.is_some());
		let inner = Arc::new(Inner {
			cached,
			connection,
			id: tg::Either::Right(id),
			instance,
			lease,
			location: location.clone(),
			outcome: Mutex::new(None),
			owned,
			state,
			stderr,
			stdin,
			stdio_task: None,
			stdout,
			task: None,
			tokens: RwLock::new(tokens),
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
	pub fn outcome_data(&self) -> Option<tg::process::outcome::Data> {
		self.0
			.outcome
			.lock()
			.unwrap()
			.as_ref()
			.map(Outcome::to_data)
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

	pub async fn detach(&self) -> tg::Result<()> {
		if let Some(connection) = &self.0.connection {
			connection.detach().await?;
		}
		self.disarm();
		if self.0.connection.is_some() {
			self.wait_stdio().await?;
		}
		Ok(())
	}

	#[must_use]
	pub(super) fn instance_with_instance<I: tg::Instance>(
		&self,
		instance: &I,
	) -> tg::instance::dynamic::Instance {
		match (&self.0.connection, self.id().right()) {
			(Some(connection), Some(id)) => tg::instance::dynamic::Instance::with_connection(
				instance.clone(),
				id.clone(),
				connection.clone(),
			),
			_ => tg::instance::dynamic::Instance::new(instance.clone()),
		}
	}

	pub(super) fn disarm(&self) {
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

	pub(crate) async fn ensure_location_with_instance<I>(&self, instance: &I) -> tg::Result<()>
	where
		I: tg::Instance,
	{
		if self.id().is_left() || self.location().is_some() {
			return Ok(());
		}
		self.try_load_with_instance(instance).await?;
		Ok(())
	}

	pub async fn load(&self) -> tg::Result<Arc<tg::process::State>> {
		let instance = tg::instance()?;
		self.load_with_instance(instance).await
	}

	pub async fn load_with_instance<I>(&self, instance: &I) -> tg::Result<Arc<tg::process::State>>
	where
		I: tg::Instance,
	{
		self.try_load_with_instance(instance)
			.await?
			.ok_or_else(|| tg::error!("failed to load the process"))
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<tg::process::State>>> {
		let instance = tg::instance()?;
		self.try_load_with_instance(instance).await
	}

	pub async fn try_load_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<Option<Arc<tg::process::State>>>
	where
		I: tg::Instance,
	{
		if let Some(mut state) = self.0.state.read().unwrap().clone() {
			let location = self.location().and_then(|location| location.to_location());
			Arc::make_mut(&mut state).inherit_location(location.as_ref());
			let tokens = self.tokens();
			Arc::make_mut(&mut state).inherit_tokens(&tokens);
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
			source: tg::process::Source::Auto,
			tokens: self.tokens(),
		};
		let Some(mut output) = instance.try_get_process(id, arg).await? else {
			return Ok(None);
		};
		if !output.tokens.is_empty() {
			let mut tokens = self.0.tokens.write().unwrap();
			output.tokens.inherit(&tokens);
			*tokens = output.tokens;
		}
		let location = output.location;
		if let Some(location) = &location {
			self.0
				.location
				.write()
				.unwrap()
				.replace(location.clone().into());
		}
		let mut state = tg::process::State::try_from(output.data)?;
		state.inherit_location(location.as_ref());
		let tokens = self.tokens();
		state.inherit_tokens(&tokens);
		let state = Arc::new(state);
		self.0.state.write().unwrap().replace(state.clone());
		Ok(Some(state))
	}

	pub async fn command(&self) -> tg::Result<tg::Either<tg::process::data::Command, tg::Command>> {
		let instance = tg::instance()?;
		self.command_with_instance(instance).await
	}

	pub async fn command_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<tg::Either<tg::process::data::Command, tg::Command>>
	where
		I: tg::Instance,
	{
		let state = self.load_with_instance(instance).await?;
		let command = match &state.command.node {
			tg::Either::Left(command) => {
				let mut command = command.as_ref().clone();
				command.inherit_location_and_tokens(&state.command.options);
				tg::Either::Left(command)
			},
			tg::Either::Right(id) => {
				let referent = tg::Referent::new(id.clone(), state.command.options.clone());
				tg::Either::Right(tg::Command::with_referent(referent))
			},
		};
		Ok(command)
	}

	pub async fn retry(&self) -> tg::Result<impl Deref<Target = bool>> {
		let instance = tg::instance()?;
		self.retry_with_instance(instance).await
	}

	pub async fn retry_with_instance<I>(
		&self,
		instance: &I,
	) -> tg::Result<impl Deref<Target = bool>>
	where
		I: tg::Instance,
	{
		Ok(self
			.load_with_instance(instance)
			.await?
			.map(|state| &state.retry))
	}

	pub async fn signal(
		&self,
		signal: tg::process::Signal,
		options: tg::process::signal::Options,
	) -> tg::Result<()> {
		let instance = tg::instance()?;
		self.signal_with_instance(instance, signal, options).await
	}

	pub async fn signal_with_instance<I>(
		&self,
		instance: &I,
		signal: tg::process::Signal,
		options: tg::process::signal::Options,
	) -> tg::Result<()>
	where
		I: tg::Instance,
	{
		let instance = self.instance_with_instance(instance);
		let instance = &instance;
		if let Some(pid) = self.id().left() {
			let pid = i32::try_from(*pid)
				.map_err(|error| tg::error!(!error, "failed to convert the process id"))?;
			let signal = i32::from(signal as u8);
			// SAFETY: The call passes only integer process and signal identifiers to libc.
			let ret = unsafe { libc::kill(pid, signal) };
			if ret < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to signal the process"
				));
			}
			return Ok(());
		}

		if self
			.0
			.connection
			.as_ref()
			.is_none_or(tg::process::connect::Connection::detached)
			&& options.location.is_none()
			&& self.location().is_none()
		{
			self.ensure_location_with_instance(instance).await?;
		}
		let arg = tg::process::signal::post::Arg {
			location: options.location.or_else(|| self.location()),
			signal,
			tokens: self.tokens(),
		};
		let id = self.id().unwrap_right();
		instance.signal_process(id, arg).await?;

		Ok(())
	}

	pub async fn output(&self, options: tg::process::wait::Options) -> tg::Result<O>
	where
		O: TryFrom<tg::Value>,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let instance = tg::instance()?;
		self.output_with_instance(instance, options).await
	}

	pub async fn output_with_instance<I>(
		&self,
		instance: &I,
		options: tg::process::wait::Options,
	) -> tg::Result<O>
	where
		I: tg::Instance,
		O: TryFrom<tg::Value>,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let outcome = self.wait_with_instance(instance, options).await?;
		let output = outcome.into_output()?;
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
		let Some(instance) = self.instance.take() else {
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
			let arg = tg::process::cancel::Arg { lease, location };
			instance.try_cancel_process(&id, arg).await.ok();
		});
	}
}
