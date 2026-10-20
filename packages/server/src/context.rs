use {tangram_client::prelude::*, tangram_futures::task::Stopper};

#[derive(Clone, Debug)]
pub struct Context {
	pub billing_ready: bool,
	pub id: Option<String>,
	pub local_process_control: bool,
	pub origin: Origin,
	pub principal: tg::Principal,

	/// The process that a runner's request acts for, which the process's control service verified.
	pub process: Option<tg::process::Id>,

	pub stopper: Option<Stopper>,
	pub token: Option<String>,
}

#[derive(Clone, Copy, Debug, derive_more::TryUnwrap, Eq, PartialEq)]
pub enum Origin {
	Host,
	Sandbox(u64),
}

impl Context {
	#[must_use]
	pub fn root() -> Self {
		Self {
			billing_ready: false,
			id: None,
			local_process_control: false,
			origin: Origin::Host,
			principal: tg::Principal::Root,
			process: None,
			stopper: None,
			token: None,
		}
	}
}
