use {
	crate::Cli, futures::FutureExt as _, tangram_client::prelude::*, tokio::io::AsyncWriteExt as _,
};

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

	/// The JS engine to use.
	#[arg(long, default_value = "auto")]
	pub engine: JsEngine,

	#[arg(index = 1)]
	pub executable: tg::command::data::Executable,

	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum JsEngine {
	#[default]
	Auto,
	QuickJs,
	V8,
}

impl Cli {
	#[must_use]
	pub fn command_js(matches: &clap::ArgMatches, args: Args) -> std::process::ExitCode {
		match Self::command_js_inner(matches, args) {
			Ok(exit) => exit,
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
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
		let logger = std::sync::Arc::new(|stream: tg::process::log::Stream, message: Vec<u8>| {
			async move {
				match stream {
					tg::process::log::Stream::Stdout => {
						tokio::io::stdout().write_all(&message).await.ok();
					},
					tg::process::log::Stream::Stderr => {
						tokio::io::stderr().write_all(&message).await.ok();
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
		runtime.block_on(client.connect())?;

		// Run.
		let future = match args.engine {
			#[cfg(feature = "v8")]
			JsEngine::Auto | JsEngine::V8 => tangram_js::v8::run(
				&client,
				args_,
				cwd,
				env,
				executable,
				logger,
				runtime.handle().clone(),
				None,
			)
			.boxed_local(),
			#[cfg(feature = "quickjs")]
			#[allow(unreachable_patterns)]
			JsEngine::Auto | JsEngine::QuickJs => tangram_js::quickjs::run(
				&client,
				args_,
				cwd,
				env,
				executable,
				logger,
				runtime.handle().clone(),
				None,
			)
			.boxed_local(),
			#[allow(unreachable_patterns)]
			_ => return Err(tg::error!("the requested JS engine is not available")),
		};
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
		if let Ok(output_path) = std::env::var("TANGRAM_OUTPUT")
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
				if let Some(data) = error
					.state()
					.object()
					.map(|object| object.unwrap_error().to_data())
				{
					let json = serde_json::to_vec(&data)
						.map_err(|source| tg::error!(!source, "failed to serialize the error"))?;
					xattr::set(&output_path, "user.tangram.error", &json)
						.map_err(|source| tg::error!(!source, "failed to write the error xattr"))?;
				} else {
					xattr::set(
						&output_path,
						"user.tangram.error",
						error.id().to_string().as_bytes(),
					)
					.map_err(|source| tg::error!(!source, "failed to write the error xattr"))?;
				}
			}
		}

		Ok(exit.into())
	}
}
