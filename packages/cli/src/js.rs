use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Set arguments as strings.
	#[arg(
		action = clap::ArgAction::Append,
		long = "arg-string",
		num_args = 1,
		short = 'a',
	)]
	pub arg_strings: Vec<String>,

	/// Set arguments as values.
	#[arg(
		action = clap::ArgAction::Append,
		long = "arg-value",
		num_args = 1,
		short = 'A',
	)]
	pub arg_values: Vec<String>,

	#[arg(index = 1)]
	pub executable: tg::command::data::Executable,

	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	#[must_use]
	pub fn command_js(matches: &clap::ArgMatches, args: Args) -> std::process::ExitCode {
		match Self::command_js_inner(matches, args) {
			Ok(exit) => exit,
			Err(error) => {
				let error = tg::Referent::with_item(error);
				Cli::print_error_basic(error);
				std::process::ExitCode::FAILURE
			},
		}
	}

	fn command_js_inner(
		matches: &clap::ArgMatches,
		args: Args,
	) -> tg::Result<std::process::ExitCode> {
		// Get the args.
		let mut args_: Vec<tg::Value> = Vec::new();
		let mut matches = matches;
		while let Some((_, matches_)) = matches.subcommand() {
			matches = matches_;
		}
		let arg_string_indices = matches.indices_of("arg_strings").unwrap_or_default();
		let arg_value_indices = matches.indices_of("arg_values").unwrap_or_default();
		let mut indexed: Vec<(usize, tg::Value)> = Vec::new();
		for (index, value) in arg_string_indices.zip(args.arg_strings) {
			let value = tg::Value::String(value);
			indexed.push((index, value));
		}
		for (index, value) in arg_value_indices.zip(args.arg_values) {
			let value = value
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the arg"))?;
			indexed.push((index, value));
		}
		indexed.sort_by_key(|&(index, _)| index);
		args_.extend(indexed.into_iter().map(|(_, value)| value));
		for arg in args.trailing {
			args_.push(tg::Value::String(arg));
		}
		let args_ = args_.iter().map(tg::Value::to_data).collect();

		// Get the cwd.
		let cwd = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;

		// Get the env.
		let env: tg::value::data::Map = std::env::vars()
			.map(|(key, value)| (key, tg::value::Data::String(value)))
			.collect();

		// Get the executable.
		let executable = args.executable;

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

		// Create the client.
		let client = tg::Client::with_env()?;

		// Create the runtime and connect the client.
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.worker_threads(1)
			.build()
			.map_err(|error| tg::error!(source = error, "failed to create the tokio runtime"))?;
		runtime.block_on(client.connect()).ok();

		// Run.
		let future = tangram_js::run(
			&client,
			args_,
			cwd,
			env,
			executable,
			logger,
			runtime.handle().clone(),
			None,
		);
		let tangram_js::Output {
			error,
			exit,
			output,
			..
		} = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.map_err(|error| tg::error!(source = error, "failed to create the tokio runtime"))?
			.block_on(future)?;

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
