use crate::Server;
use futures::FutureExt as _;
use tangram_client as tg;

mod progress;
mod proxy;
mod util;

pub mod builtin;
#[cfg(target_os = "macos")]
pub mod darwin;
#[cfg(feature = "v8")]
pub mod js;
#[cfg(target_os = "linux")]
pub mod linux;

#[derive(Clone)]
pub enum Runtime {
	Builtin(builtin::Runtime),
	#[cfg(target_os = "macos")]
	Darwin(darwin::Runtime),
	#[cfg(feature = "v8")]
	Js(js::Runtime),
	#[cfg(target_os = "linux")]
	Linux(linux::Runtime),
}

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	#[allow(clippy::struct_field_names)]
	pub output: Option<tg::Value>,
}

impl Runtime {
	pub fn server(&self) -> &Server {
		match self {
			Self::Builtin(s) => &s.server,
			#[cfg(target_os = "macos")]
			Self::Darwin(s) => &s.server,
			#[cfg(feature = "v8")]
			Self::Js(s) => &s.server,
			#[cfg(target_os = "linux")]
			Self::Linux(s) => &s.server,
		}
	}

	pub async fn run(&self, process: &tg::Process) -> Output {
		match self.run_inner(process).await {
			Ok(output) => output,
			Err(error) => Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			},
		}
	}

	async fn run_inner(&self, process: &tg::Process) -> tg::Result<Output> {
		// Ensure the process is loaded.
		let state = process.load(self.server()).await?;

		// Run the process.
		let mut wait = match self {
			Runtime::Builtin(runtime) => runtime.run(process).boxed().await,
			#[cfg(target_os = "macos")]
			Runtime::Darwin(runtime) => runtime.run(process).boxed().await,
			#[cfg(feature = "v8")]
			Runtime::Js(runtime) => runtime.run(process).boxed().await,
			#[cfg(target_os = "linux")]
			Runtime::Linux(runtime) => runtime.run(process).boxed().await,
		};

		// Compute the checksum if necessary.
		if let (Some(checksum), None, Some(value)) =
			(&state.expected_checksum, &wait.checksum, &wait.output)
		{
			let algorithm = checksum.algorithm();
			let checksum = self.compute_checksum(value, algorithm).await?;
			wait.checksum = Some(checksum);
		}

		Ok(wait)
	}

	async fn compute_checksum(
		&self,
		value: &tg::Value,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		if let Ok(blob) = value.clone().try_into() {
			self.server().checksum_blob(&blob, algorithm).await
		} else if let Ok(artifact) = value.clone().try_into() {
			self.server().checksum_artifact(&artifact, algorithm).await
		} else {
			Err(tg::error!(
				"cannot checksum a value that is not a blob or an artifact"
			))
		}
	}
}
