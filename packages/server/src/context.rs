use {std::sync::Arc, tangram_client::prelude::*, tangram_futures::task::Stopper};

#[derive(Clone, Debug)]
pub struct Context {
	pub authentication: Authentication,
	pub id: Option<tg::Id>,
	pub process: Option<Arc<Process>>,
	pub sandbox: Option<tg::sandbox::Id>,
	pub stopper: Option<Stopper>,
	pub token: Option<String>,
	pub untrusted: bool,
}

#[derive(Clone, Debug, Default)]
pub enum Authentication {
	Authenticated(tg::User),
	Root,
	#[default]
	Unauthenticated,
}

#[derive(Clone, Debug)]
pub struct Process {
	pub debug: Option<tg::process::Debug>,
	pub id: tg::process::Id,
	pub location: Option<tg::location::Location>,
	pub retry: bool,
}

impl Context {
	#[must_use]
	pub fn root() -> Self {
		Self {
			authentication: Authentication::Root,
			id: None,
			process: None,
			sandbox: None,
			stopper: None,
			token: None,
			untrusted: false,
		}
	}
}

impl Default for Context {
	fn default() -> Self {
		Self::root()
	}
}
