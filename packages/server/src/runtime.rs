use futures::Future;
use tangram_client as tg;
use tangram_error::Result;

#[cfg(target_os = "macos")]
pub mod darwin;
pub mod js;
#[cfg(target_os = "linux")]
pub mod linux;
mod proxy;
pub mod util;

pub trait Trait: Clone {
	fn run(&self, build: &tg::Build) -> impl Future<Output = Result<tg::Value>> + Send;
}

#[derive(Clone)]
pub enum Runtime {
	#[cfg(target_os = "macos")]
	Darwin(darwin::Runtime),
	Js(js::Runtime),
	#[cfg(target_os = "linux")]
	Linux(linux::Runtime),
}

impl Trait for Runtime {
	async fn run(&self, build: &tg::Build) -> Result<tg::Value> {
		match self {
			#[cfg(target_os = "macos")]
			Runtime::Darwin(runtime) => runtime.run(build).await,
			Runtime::Js(runtime) => runtime.run(build).await,
			#[cfg(target_os = "linux")]
			Runtime::Linux(runtime) => runtime.run(build).await,
		}
	}
}
