use {crate::Cli, futures::FutureExt as _, tangram_client as tg};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	host: String,

	#[arg(long)]
	temp_path: std::path::PathBuf,
}

impl Cli {
	#[must_use]
	pub fn command_internal_run(args: Args) -> std::process::ExitCode {
		match Self::command_internal_run_inner(args) {
			Ok(exit) => exit,
			Err(error) => {
				let error = tg::Referent::with_item(error);
				Cli::print_error_basic(error);
				std::process::ExitCode::FAILURE
			},
		}
	}

	fn command_internal_run_inner(args: Args) -> tg::Result<std::process::ExitCode> {
		let Args { host, temp_path } = args;

		// Create the client.
		let client = tg::Client::with_env()?;

		// Get the current process.
		let process = tg::Process::current()?
			.ok_or_else(|| tg::error!("the TANGRAM_PROCESS environment variable is not set"))?;

		// Create a logger that writes to stdio.
		let logger = std::sync::Arc::new(
			|_process: &tg::Process, stream: tg::process::log::Stream, string: String| {
				async move {
					match stream {
						tg::process::log::Stream::Stdout => {
							print!("{string}");
						},
						tg::process::log::Stream::Stderr => {
							eprint!("{string}");
						},
					}
					Ok(())
				}
				.boxed()
			},
		);

		// Run based on the host and convert to a common output structure.
		let (_checksum, error, exit, output_value) = match host.as_str() {
			"builtin" => {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.map_err(|error| {
						tg::error!(source = error, "failed to create the tokio runtime")
					})?;
				let output = runtime
					.block_on(tangram_builtin::run(&client, &process, logger, &temp_path))?;
				(output.checksum, output.error, output.exit, output.output)
			},

			#[cfg(feature = "js")]
			"js" => {
				let main_runtime = tokio::runtime::Builder::new_multi_thread()
					.enable_all()
					.worker_threads(1)
					.build()
					.map_err(|error| {
						tg::error!(source = error, "failed to create the tokio runtime")
					})?;
				main_runtime.block_on(client.connect())?;
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.map_err(|error| {
						tg::error!(source = error, "failed to create the tokio runtime")
					})?;
				let output = runtime.block_on(tangram_js::run(
					&client,
					&process,
					logger,
					main_runtime.handle().clone(),
					None,
				))?;
				(output.checksum, output.error, output.exit, output.output)
			},

			_ => return Err(tg::error!("unsupported host")),
		};

		// Write the output.
		let output_path = std::env::var("OUTPUT")
			.map_err(|source| tg::error!(!source, "expected $OUTPUT to be set"))?;
		if output_value.is_some() || error.is_some() {
			std::fs::write(&output_path, "")
				.map_err(|source| tg::error!(!source, "failed to write the output"))?;
			if let Some(output) = &output_value {
				let tgon = output.to_string();
				xattr::set(&output_path, "user.tangram.output", tgon.as_bytes())
					.map_err(|source| tg::error!(!source, "failed to write the output xattr"))?;
			}
			if let Some(error) = &error {
				let json = serde_json::to_vec(&error.to_data())
					.map_err(|source| tg::error!(!source, "failed to serialize the error"))?;
				xattr::set(&output_path, "user.tangram.error", &json)
					.map_err(|source| tg::error!(!source, "failed to write the error xattr"))?;
			}
		}

		Ok(exit.into())
	}
}
