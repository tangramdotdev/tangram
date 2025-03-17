use crate::Server;
use futures::{FutureExt as _, TryFutureExt, future};
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};
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
		// Attempt to reuse an existing process.
		if let Some(output) = self.try_reuse_process(process).boxed().await? {
			return Ok(output);
		}

		// Ensure the process is loaded.
		let state = process.load(self.server()).await?;

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
		if let (Some(value), Some(checksum)) = (&output.output, &state.checksum) {
			self::util::compute_checksum(self, process, value, checksum).await?;
		}

		Ok(output)
	}

	async fn try_reuse_process(&self, process: &tg::Process) -> tg::Result<Option<Output>> {
		// Load the process.
		let Ok(state) = process.load(self.server()).await else {
			return Ok(None);
		};

		// If there is no checksum, then return.
		let Some(checksum) = &state.checksum else {
			return Ok(None);
		};

		// If the process is not cacheable, then return.
		if !state.cacheable {
			return Ok(None);
		}

		// If the process checksum was "none" or "any", then return.
		if matches!(checksum, tg::Checksum::None) || matches!(checksum, tg::Checksum::Any) {
			return Ok(None);
		}

		// Try spawning the command with the checksum "none".
		let arg = tg::process::spawn::Arg {
			checksum: Some(tangram_client::Checksum::None),
			command: state.command.id(self.server()).await.ok(),
			create: false,
			cwd: None,
			env: None,
			mounts: Vec::new(),
			network: false,
			parent: None,
			remote: process.remote().cloned(),
			retry: state.retry,
			stderr: None,
			stdin: None,
			stdout: None,
		};

		// Spawn the process.
		let Ok(Some(spawn_output)) = self.server().try_spawn_process(arg).await else {
			return Ok(None);
		};
		let existing_process_id = spawn_output.process;

		// Spawn a task to copy the children.
		let children_task = tokio::spawn({
			let server = self.server().clone();
			let process = process.clone();
			let existing_process_id = existing_process_id.clone();
			async move {
				let arg = tg::process::children::get::Arg::default();
				let stream = server
					.get_process_children(&existing_process_id, arg)
					.await?;
				let mut stream = pin!(stream);
				while let Some(chunk) = stream.try_next().await? {
					for child in chunk.data {
						if let Some(child_process) = server.try_get_process(&child).await? {
							let arg = tg::process::spawn::Arg {
								checksum: child_process.data.checksum,
								command: Some(child_process.data.command),
								create: false,
								cwd: None,
								env: None,
								mounts: child_process.data.mounts,
								network: child_process.data.network,
								parent: Some(process.id().clone()),
								remote: process.remote().cloned(),
								retry: child_process.data.retry,
								stderr: None,
								stdin: None,
								stdout: None,
							};
							server.try_spawn_process(arg).await?;
						}
					}
				}
				Ok::<_, tg::Error>(())
			}
		})
		.map_err(|source| tg::error!(!source, "the log task panicked"))
		.and_then(future::ready);

		// Spawn a task to copy the log.
		let log_task = tokio::spawn({
			let server = self.server().clone();
			let process = process.clone();
			let existing_process_id = existing_process_id.clone();
			async move {
				let arg = tg::process::log::get::Arg {
					remote: process.remote().cloned(),
					..Default::default()
				};
				let stream = server.get_process_log(&existing_process_id, arg).await?;
				let mut stream = pin!(stream);
				while let Some(chunk) = stream.try_next().await? {
					let arg = tg::process::log::post::Arg {
						bytes: chunk.bytes,
						remote: process.remote().cloned(),
					};
					server.try_post_process_log(process.id(), arg).await?;
				}
				Ok::<_, tg::Error>(())
			}
		})
		.map_err(|source| tg::error!(!source, "the log task panicked"))
		.and_then(future::ready);

		// Spawn a task to await the process.
		let wait_task = tokio::spawn({
			let server = self.server().clone();
			let existing_process_id = existing_process_id.clone();
			async move {
				let output = server.wait_process(&existing_process_id).await?;
				Ok::<_, tg::Error>(output)
			}
		})
		.map_err(|source| tg::error!(!source, "the log task panicked"))
		.and_then(future::ready);

		// Join the tasks.
		let ((), (), output) = futures::try_join!(children_task, log_task, wait_task)?;

		// If the process did not succeed, then return its output.
		if !output.status.is_succeeded() {
			let value = output.output.map(tg::Value::try_from).transpose()?;
			let output = Output {
				error: None,
				exit: output.exit,
				output: value,
			};
			return Ok(Some(output));
		}

		// If the process had no output or the output cannot be converted to a `tg::Value`, return.
		let value = output
			.output
			.ok_or_else(|| tg::error!("expected the output to be set"))?;
		let value = tg::Value::try_from(value)?;

		// Compute the checksum.
		let actual_checksum = self::util::compute_checksum(self, process, &value, checksum).await?;

		// Verify the checksum matches the expected value.
		if *checksum != actual_checksum {
			let error =
				tg::error!("checksums do not match, expected {checksum}, actual {actual_checksum}");
			let output = Output {
				error: Some(error),
				exit: output.exit,
				output: None,
			};
			return Ok(Some(output));
		}

		// Create the output.
		let output = Output {
			error: None,
			exit: output.exit,
			output: Some(value),
		};

		Ok(Some(output))
	}
}
