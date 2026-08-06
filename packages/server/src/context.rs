use {tangram_client::prelude::*, tangram_futures::task::Stopper};

#[derive(Clone, Debug)]
pub struct Context {
	pub billing: bool,
	pub id: Option<String>,
	pub origin: Origin,
	pub principal: tg::Principal,
	pub stopper: Option<Stopper>,
	pub token: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Origin {
	Host,
	Sandbox { index: u64 },
}

impl Context {
	#[must_use]
	pub fn root() -> Self {
		Self {
			billing: false,
			id: None,
			origin: Origin::Host,
			principal: tg::Principal::Root,
			stopper: None,
			token: None,
		}
	}
}
