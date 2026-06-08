use {tangram_client::prelude::*, tangram_futures::task::Stopper};

#[derive(Clone, Debug)]
pub struct Context {
	pub id: Option<String>,
	pub principal: Option<tg::Principal>,
	pub sandbox: bool,
	pub stopper: Option<Stopper>,
}

impl Context {
	#[must_use]
	pub fn root() -> Self {
		Self {
			id: None,
			principal: Some(tg::Principal::Root),
			sandbox: false,
			stopper: None,
		}
	}
}
