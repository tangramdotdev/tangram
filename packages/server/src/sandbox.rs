use {
	crate::temp::Temp, std::sync::Arc, tangram_futures::task::Task, tangram_sandbox as sandbox,
	tangram_uri::Uri,
};

pub mod create;
pub mod delete;

pub struct Sandbox {
	pub client: Arc<sandbox::client::Client>,
	pub context: crate::Context,
	pub process: tokio::process::Child,
	#[cfg_attr(
		target_os = "linux",
		allow(dead_code, reason = "the proxy URL is only used on darwin")
	)]
	pub proxy_url: Option<Uri>,
	pub refcount: usize,
	pub serve_task: Task<()>,
	#[allow(dead_code, reason = "owns the piped stderr fd for the sandbox daemon")]
	pub stderr: Option<tokio::process::ChildStderr>,
	#[allow(dead_code, reason = "the temp directory must be kept alive")]
	pub temp: Temp,
}
