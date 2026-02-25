use {
	crate::temp::Temp,
	std::{path::PathBuf, sync::Arc},
	tangram_futures::task::Task,
	tangram_sandbox as sandbox,
};

pub mod create;
#[cfg(target_os = "macos")]
mod darwin;
pub mod delete;
#[cfg(target_os = "linux")]
mod linux;
pub mod spawn;
pub mod wait;

pub struct Sandbox {
	pub process: tokio::process::Child,
	pub client: Arc<sandbox::client::Client>,
	#[allow(dead_code, reason = "required by darwin")]
	pub root: PathBuf,
	pub serve_task: Task<()>,
	pub _temp: Temp,
}
