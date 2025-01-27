use crate::{compiler::Compiler, Server};
use futures::FutureExt as _;
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

#[derive(Debug)]
pub struct Output {
	pub error: Option<tg::Error>,
	pub exit: Option<tg::process::Exit>,
	pub value: Option<tg::Value>,
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

	pub async fn run(
		&self,
		process: &tg::process::get::Output,
		command: &tg::Command,
		remote: Option<&String>,
	) -> Output {
		// Run the command.
		let mut output = match self {
			Runtime::Builtin(runtime) => {
				runtime.run(process, command, remote.cloned()).boxed().await
			},
			#[cfg(target_os = "macos")]
			Runtime::Darwin(runtime) => runtime.run(process, command, remote.cloned()).boxed().await,
			Runtime::Js(runtime) => runtime.run(process, command, remote.cloned()).boxed().await,
			#[cfg(target_os = "linux")]
			Runtime::Linux(runtime) => runtime.run(process, command, remote.cloned()).boxed().await,
		};

		// If the process has a checksum, then compute the checksum of the output.
		if let (Some(value), None, Some(checksum)) =
			(&output.value, &output.error, &process.checksum)
		{
			let result = self::util::compute_checksum(self, &process.id, value, checksum).await;
			if let Err(error) = result {
				output.error = Some(error);
			}
		}

		// If there is an error, then add it to the process's log.
		if let Some(error) = &output.error {
			let arg = tg::process::log::post::Arg {
				bytes: error.to_string().into(),
				remote: remote.cloned(),
			};
			self.server()
				.try_post_process_log(&process.id, arg)
				.await
				.ok();
		}

		output
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
