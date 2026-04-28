use {std::sync::Arc, tangram_client::prelude::*, tangram_futures::task::Stopper};

#[derive(Clone, Debug, Default)]
pub struct Context {
	pub id: Option<tg::Id>,
	pub process: Option<Arc<Process>>,
	pub sandbox: Option<tg::sandbox::Id>,
	pub stopper: Option<Stopper>,
	pub token: Option<String>,
	pub untrusted: bool,
}

#[derive(Clone, Debug)]
pub struct Process {
	pub debug: Option<tg::process::Debug>,
	pub id: tg::process::Id,
	pub location: Option<tg::location::Location>,
	pub retry: bool,
}
