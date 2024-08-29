use crate::{compiler::Compiler, Server};
use tangram_client as tg;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

mod proxy;
mod util;

pub mod builtin;
#[cfg(target_os = "macos")]
pub mod darwin;
pub mod js;
#[cfg(target_os = "linux")]
pub mod linux;

#[derive(Clone)]
pub enum Runtime {
	Builtin(builtin::Runtime),
	#[cfg(target_os = "macos")]
	Darwin(darwin::Runtime),
	Js(js::Runtime),
	#[cfg(target_os = "linux")]
	Linux(linux::Runtime),
}

impl Runtime {
	pub async fn build(&self, build: &tg::Build, remote: Option<String>) -> tg::Result<tg::Value> {
		match self {
			Runtime::Builtin(runtime) => runtime.build(build, remote).await,
			#[cfg(target_os = "macos")]
			Runtime::Darwin(runtime) => runtime.build(build, remote).await,
			Runtime::Js(runtime) => runtime.build(build, remote).await,
			#[cfg(target_os = "linux")]
			Runtime::Linux(runtime) => runtime.build(build, remote).await,
		}
	}
}

impl Server {
	pub async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		// Create the module.
		let kind = tg::module::Kind::Dts;
		let path = "tangram.d.ts".into();
		let module = tg::Module::new(kind, None, Some(path));

		// Create the compiler.
		let compiler = Compiler::new(self, tokio::runtime::Handle::current());

		// Get the doc.
		let doc = compiler.document(&module).await?;

		Ok(doc)
	}
}

impl Server {
	pub(crate) async fn handle_get_js_runtime_doc_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let output = handle.get_js_runtime_doc().await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
