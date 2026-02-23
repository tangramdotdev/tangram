use {
	crate::temp::Temp,
	std::{path::PathBuf, sync::Arc},
	tangram_sandbox as sandbox,
};

pub mod create;
pub mod delete;
mod linux;
pub mod spawn;
pub mod wait;

pub struct Sandbox {
	pub process: tokio::process::Child,
	pub client: Arc<sandbox::client::Client>,
	pub root: PathBuf,
	pub _temp: Temp,
}
