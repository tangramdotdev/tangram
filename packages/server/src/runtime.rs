use crate::{compiler::Compiler, Server};
use futures::FutureExt as _;
use std::pin::pin;
use tangram_client as tg;
use tangram_http::{response::builder::Ext as _, Body};
use tokio_stream::StreamExt as _;

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
		if let Some(existing_output) = self.try_locate_existing_process(process).await {
			return existing_output;
		}

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

	async fn try_locate_existing_process(&self, process: &tg::Process) -> Option<Output> {
		// If the checksum is anything other than Any or None, spawn with create: false an identical process with checksum: None.

		// Load the process state.
		let Ok(state) = process.load(self.server()).await else {
			return None;
		};

		// If there's no checksum, return.
		let Some(checksum) = &state.checksum else {
			return None;
		};

		// If the process is not cacheable, return.
		if state.cwd.is_some() || state.env.is_some() || state.network {
			return None;
		}

		// If the process checksum was None or Any, return.
		if matches!(checksum, tg::Checksum::None) || matches!(checksum, tg::Checksum::Any) {
			return None;
		}

		// Try spawning the command with `Checksum::None`.
		let arg = tg::process::spawn::Arg {
			checksum: Some(tangram_client::Checksum::None),
			command: state.command.id(self.server()).await.ok(),
			create: false,
			cwd: None,
			env: None,
			network: false,
			parent: None,
			remote: process.remote().cloned(),
			retry: state.retry,
		};

		// If no match was found, return.
		let Ok(Some(spawn_output)) = self.server().try_spawn_process(arg).await else {
			return None;
		};

		// Get the process.
		let Ok(Some(checksum_none_process)) =
			self.server().try_get_process(&spawn_output.process).await
		else {
			return None;
		};

		// If the process had no output or the output cannot be converted to a `tg::Value`, return.
		let checksum_none_output_value: tg::Value =
			checksum_none_process.output?.try_into().ok()?;

		// Compute the checksum.
		let actual_checksum =
			self::util::compute_checksum(self, process, &checksum_none_output_value, checksum)
				.await
				.ok()?;
		// Verify the checksum matches the expected value.
		if *checksum != actual_checksum {
			return Some(Output {
				error: Some(tg::error!(
					"checksums do not match, expected {checksum}, actual {actual_checksum}"
				)),
				exit: checksum_none_process.exit,
				output: None,
			});
		}

		// Update the calling process to use the log of the `Checksum::None` process.
		if checksum_none_process.log.is_some() {
			if let Some(log_stream) = self
				.server()
				.try_get_process_log_stream(
					&checksum_none_process.id,
					tg::process::log::get::Arg {
						remote: process.remote().cloned(),
						..Default::default()
					},
				)
				.await
				.ok()?
			{
				let mut stream = pin!(log_stream);
				while let Some(Ok(event)) = stream.next().await {
					match event {
						tangram_client::process::log::get::Event::Chunk(chunk) => {
							let arg = tg::process::log::post::Arg {
								bytes: chunk.bytes,
								remote: process.remote().cloned(),
							};
							self.server()
								.try_post_process_log(process.id(), arg)
								.await
								.ok();
						},
						tangram_client::process::log::get::Event::End => break,
					}
				}
			}
		}

		// Copy the children.
		if let Some(stream) = self
			.server()
			.try_get_process_children_stream(
				&checksum_none_process.id,
				tg::process::children::get::Arg::default(),
			)
			.await
			.ok()?
		{
			let mut stream = pin!(stream);
			while let Some(event) = stream.next().await {
				let event = event.unwrap();
				match event {
					tangram_client::process::children::get::Event::Chunk(chunk) => {
						for child_id in chunk.data {
							if let Some(child_process) =
								self.server().try_get_process(&child_id).await.ok()?
							{
								let arg = tg::process::spawn::Arg {
									checksum: child_process.checksum,
									command: Some(child_process.command),
									create: false,
									cwd: None,
									env: None,
									network: child_process.network,
									parent: Some(process.id().clone()),
									remote: process.remote().cloned(),
									retry: child_process.retry,
								};
								self.server().try_spawn_process(arg).await.ok()?;
							}
						}
					},
					tangram_client::process::children::get::Event::End => break,
				}
			}
		}

		Some(Output {
			error: None,
			exit: checksum_none_process.exit,
			output: Some(checksum_none_output_value),
		})
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
