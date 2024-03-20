use async_trait::async_trait;
use tangram_client as tg;
use tangram_error::Result;

#[cfg(target_os = "macos")]
pub mod darwin;
pub mod js;
#[cfg(target_os = "linux")]
pub mod linux;
mod proxy;
pub mod util;

#[async_trait]
pub trait Runtime: Send + Sync + 'static {
	fn clone_box(&self) -> Box<dyn Runtime>;
	async fn run(&self, build: &tg::Build) -> Result<tg::Value>;
}
