use {std::sync::Arc, tangram_client::prelude::*};

#[derive(Clone, Debug, Default)]
pub struct Context {
	pub process: Option<Arc<Process>>,
	pub sandbox: Option<tg::sandbox::Id>,
	pub token: Option<String>,
	pub untrusted: bool,
}

#[derive(Clone, Debug)]
pub struct Process {
	pub id: tg::process::Id,
	pub remote: Option<String>,
	pub retry: bool,
}
