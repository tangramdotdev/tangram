use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub executable: Option<tg::Reference>,

	#[arg(long, conflicts_with_all = ["executable", "trailing"])]
	pub process: Option<tg::process::Id>,

	#[arg(long)]
	pub temp_path: std::path::PathBuf,

	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	#[must_use]
	pub fn command_builtin(args: Args) -> std::process::ExitCode {
		match Self::command_builtin_inner(args) {
			Ok(exit) => exit,
			Err(error) => {
				let error = tg::Referent::with_item(error);
				Cli::print_error_basic(error);
				std::process::ExitCode::FAILURE
			},
		}
	}

	fn command_builtin_inner(args: Args) -> tg::Result<std::process::ExitCode> {
		// Create the runtime.
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.map_err(|error| tg::error!(source = error, "failed to create the tokio runtime"))?;

		// Create the client.
		let client = tg::Client::with_env()?;
		runtime.block_on(client.connect())?;

		// Get the current tangram process if there is one.
		let process = args
			.process
			.map(|process| tg::Process::new(process, None, None, None, None));

		// Get the executable and args from the process if it exists.
		let (args_, cwd, env, executable) = if let Some(process) = &process {
			runtime.block_on(async {
				let command = process.command(&client).await?;
				let data = command.data(&client).await?;
				let args = data.args;
				let cwd = if let Some(cwd) = data.cwd {
					cwd
				} else {
					std::env::current_dir().map_err(|source| {
						tg::error!(!source, "failed to get the current directory")
					})?
				};
				let env = data.env;
				let executable = data.executable;
				Ok::<_, tg::Error>((args, cwd, env, executable))
			})?
		} else {
			// Get the cwd and env and add to the args and update the executable.
			let args_ = args
				.trailing
				.into_iter()
				.map(tg::value::Data::String)
				.collect();
			let cwd = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
			let env: tg::value::data::Map = std::env::vars()
				.map(|(key, value)| (key, tg::value::Data::String(value)))
				.collect();
			let executable = args
				.executable
				.ok_or_else(|| tg::error!("expected an executable"))?;
			let executable = executable
				.item()
				.try_unwrap_path_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a path"))?
				.clone();
			let executable =
				tg::command::data::Executable::Path(tg::command::data::PathExecutable {
					path: executable,
				});
			(args_, cwd, env, executable)
		};

		// Create a logger that writes to stdio.
		let logger = std::sync::Arc::new(|stream: tg::process::log::Stream, string: String| {
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
		});

		// Run.
		let tangram_builtin::Output {
			error,
			exit,
			output,
			..
		} = runtime.block_on(tangram_builtin::run(
			&client,
			process.as_ref(),
			args_,
			cwd,
			env,
			executable,
			logger,
			&args.temp_path,
		))?;

		// Write the output.
		if let Ok(output_path) = std::env::var("OUTPUT")
			&& (output.is_some() || error.is_some())
		{
			std::fs::write(&output_path, "")
				.map_err(|source| tg::error!(!source, "failed to write the output"))?;
			if let Some(output) = &output {
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
