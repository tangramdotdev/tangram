use crate::{
	util::http::{full, Incoming, Outgoing},
	Server,
};
use futures::Future;
use tangram_client as tg;

mod proxy;
mod util;

#[cfg(target_os = "macos")]
pub mod darwin;
pub mod js;
#[cfg(target_os = "linux")]
pub mod linux;

pub trait Trait: Clone {
	fn run(&self, build: &tg::Build) -> impl Future<Output = tg::Result<tg::Value>> + Send;
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
	async fn run(&self, build: &tg::Build) -> tg::Result<tg::Value> {
		match self {
			#[cfg(target_os = "macos")]
			Runtime::Darwin(runtime) => runtime.run(build).await,
			Runtime::Js(runtime) => runtime.run(build).await,
			#[cfg(target_os = "linux")]
			Runtime::Linux(runtime) => runtime.run(build).await,
		}
	}
}

impl Server {
	pub async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		// Create the module.
		let module = tg::Module::Library(tg::module::Library {
			path: "tangram.d.ts".parse().unwrap(),
		});

		// Create the language server.
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());

		// Get the doc.
		let doc = language_server.doc(&module).await?;

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

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
