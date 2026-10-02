use {
	super::Id,
	crate::prelude::*,
	std::sync::{
		Arc, RwLock,
		atomic::{AtomicBool, Ordering},
	},
};

#[derive(Clone, Debug)]
pub struct Sandbox(pub(super) Arc<Inner>);

#[derive(derive_more::Debug)]
pub(super) struct Inner {
	id: Id,
	#[debug(ignore)]
	instance: Option<tg::instance::dynamic::Instance>,
	pub(super) location: RwLock<Option<tg::location::Arg>>,
	owned: AtomicBool,
	pub(super) state: RwLock<Option<Arc<tg::sandbox::get::Output>>>,
	pub(super) tokens: RwLock<tg::authorization::Tokens>,
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
	pub(super) fn new_inner(
		id: Id,
		options: tg::sandbox::Options,
		instance: Option<tg::instance::dynamic::Instance>,
	) -> Self {
		let tg::sandbox::Options {
			location,
			state,
			tokens,
		} = options;
		let location = RwLock::new(location);
		let owned = AtomicBool::new(instance.is_some());
		let mut tokens = tokens;
		tokens.normalize(None);
		if let Some(state) = &state {
			tokens.inherit(&state.tokens);
		}
		let state = RwLock::new(state.map(Arc::new));
		let tokens = RwLock::new(tokens);
		let inner = Inner {
			id,
			instance,
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

	#[must_use]
	pub fn to_referent(&self) -> tg::Referent<Id> {
		let options = tg::referent::Options {
			location: self.location().and_then(|location| location.to_location()),
			tokens: self.tokens(),
			..tg::referent::Options::default()
		};
		tg::Referent::new(self.id().clone(), options)
	}

	pub fn detach(&self) {
		self.0.owned.store(false, Ordering::SeqCst);
	}

	pub async fn run(&self, arg: tg::process::Arg) -> tg::Result<tg::Value> {
		let instance = tg::instance()?;
		self.run_with_instance(instance, arg).await
	}

	pub async fn run_with_instance<I>(
		&self,
		instance: &I,
		mut arg: tg::process::Arg,
	) -> tg::Result<tg::Value>
	where
		I: tg::Instance,
	{
		arg.location = self.location().or(arg.location);
		arg.sandbox = Some(tg::process::SandboxArg::Referent(self.to_referent()));

		tg::Process::<tg::Value>::run_with_instance(instance, arg).await
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
		let Some(instance) = self.instance.take() else {
			return;
		};
		let id = self.id.clone();
		let location = self.location.read().unwrap().clone();
		let tokens = self.tokens.read().unwrap().clone();
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		runtime.spawn(async move {
			let arg = tg::sandbox::destroy::Arg {
				error: None,
				location,
				tokens,
			};
			instance.try_destroy_sandbox(&id, arg).await.ok();
		});
	}
}
