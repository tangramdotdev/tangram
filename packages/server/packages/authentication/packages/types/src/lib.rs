use tangram_client::prelude::*;

pub mod cache;
pub mod token;

#[derive(Clone)]
pub struct Entry {
	pub billing_ready: Option<bool>,
	pub principal: tg::Principal,
}

#[derive(Clone)]
pub struct Process {
	pub debug: Option<tg::process::Debug>,
	pub host: String,
	pub inner_token: Option<String>,
	pub location: Option<tg::Location>,
	pub retry: bool,
	pub sandbox: tg::sandbox::Id,
}

/// The token with which a principal authenticates to a remote, and the process or sandbox that a runner's token claims to act for.
#[derive(Clone, Default)]
pub struct RemoteToken {
	pub process: Option<tg::process::Id>,
	pub sandbox: Option<tg::sandbox::Id>,
	pub token: Option<String>,
}

#[derive(Clone)]
pub struct Sandbox {
	pub location: tg::Location,
	pub token: Option<String>,
}

impl RemoteToken {
	#[must_use]
	pub fn with_token(token: String) -> Self {
		Self {
			process: None,
			sandbox: None,
			token: Some(token),
		}
	}
}
