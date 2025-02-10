use crate::{compiler::Compiler, Server};
use futures::FutureExt as _;
use tangram_client as tg;
use tangram_http::{response::builder::Ext as _, Body};

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

#[derive(Debug)]
pub struct Output {
	pub error: Option<tg::Error>,
	pub exit: Option<tg::process::Exit>,
	#[allow(clippy::struct_field_names)]
	pub output: Option<tg::Value>,
}

impl Runtime {
	pub fn server(&self) -> &Server {
		match self {
			Self::Builtin(s) => &s.server,
			Self::Js(s) => &s.server,
			#[cfg(target_os = "linux")]
			Self::Linux(s) => &s.server,
			#[cfg(target_os = "macos")]
			Self::Darwin(s) => &s.server,
		}
	}

	pub async fn run(&self, process: &tg::Process) -> Output {
		let output = match self.run_inner(process).await {
			Ok(output) => output,
			Err(error) => Output {
				error: Some(error),
				exit: None,
				output: None,
			},
		};

		// If there is an error, then add it to the process's log.
		if let Some(error) = &output.error {
			let arg = tg::process::log::post::Arg {
				bytes: error.to_string().into(),
				remote: process.remote().cloned(),
			};
			self.server()
				.try_post_process_log(process.id(), arg)
				.await
				.ok();
		}

		output
	}

	async fn run_inner(&self, process: &tg::Process) -> tg::Result<Output> {
		// Run the process.
		let output = match self {
			Runtime::Builtin(runtime) => runtime.run(process).boxed().await,
			#[cfg(target_os = "macos")]
			Runtime::Darwin(runtime) => runtime.run(process).boxed().await,
			Runtime::Js(runtime) => runtime.run(process).boxed().await,
			#[cfg(target_os = "linux")]
			Runtime::Linux(runtime) => runtime.run(process).boxed().await,
		};

		// If the process has a checksum, then compute the checksum of the output.
		let state = process.load(self.server()).await?;
		if let (Some(value), Some(checksum)) = (&output.output, &state.checksum) {
			self::util::compute_checksum(self, process, value, checksum).await?;
		}

		Ok(output)
	}
}

impl Server {
	pub async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		// Create the module.
		let module = tg::Module {
			kind: tg::module::Kind::Dts,
			referent: tg::Referent {
				item: tg::module::Item::Path("tangram.d.ts".into()),
				path: None,
				subpath: None,
				tag: None,
			},
		};

		// Create the compiler.
		let compiler = Compiler::new(self, tokio::runtime::Handle::current());

		// Document the module.
		let document = compiler.document(&module).await?;

		// Stop and await the compiler.
		compiler.stop();
		compiler.wait().await;

		Ok(document)
	}
}

impl Server {
	pub(crate) async fn handle_get_js_runtime_doc_request<H>(
		handle: &H,
		_request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let output = handle.get_js_runtime_doc().await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
