use {crate::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Set arguments as strings.
	#[arg(
		action = clap::ArgAction::Append,
		allow_hyphen_values = true,
		long = "arg-string",
		num_args = 1,
		short = 'a',
	)]
	pub arg_strings: Vec<String>,

	/// Set arguments as values.
	#[arg(
		action = clap::ArgAction::Append,
		allow_hyphen_values = true,
		long = "arg-value",
		num_args = 1,
		short = 'A',
	)]
	pub arg_values: Vec<String>,

	#[arg(long)]
	pub export: Option<String>,

	#[arg(index = 1)]
	pub module: String,

	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_python(&mut self, args: Args) -> tg::Result<()> {
		let exit = self.command_python_inner(args).await?;
		self.exit.replace(exit.into());
		Ok(())
	}

	async fn command_python_inner(&self, args: Args) -> tg::Result<u8> {
		// Get the args.
		let mut args_: Vec<tg::Value> = Vec::new();
		let mut matches = &self.matches;
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
				.map_err(|error| tg::error!(!error, arg = %value, "failed to parse the arg"))?;
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
			.map_err(|error| tg::error!(!error, "failed to get the current directory"))?;

		// Get the env.
		let env = tg::process::env()?
			.into_iter()
			.map(|(key, value)| (key, value.to_data()))
			.collect();

		// Get the module.
		let module = if args.module.starts_with("tg.module(") {
			args.module
				.parse::<tg::Value>()?
				.try_unwrap_module()
				.map_err(|_| tg::error!("expected a python module"))?
				.to_data()
		} else {
			let path = std::fs::canonicalize(&args.module)
				.map_err(|error| tg::error!(!error, "failed to canonicalize the module path"))?;
			let name = path
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap_or_default();
			if name != "tangram.py" && !name.ends_with(".tg.py") {
				return Err(tg::error!("expected tangram.py or a .tg.py module"));
			}
			tg::module::Data {
				kind: tg::module::Kind::Python,
				referent: tg::Referent::with_node(tg::module::data::Source::Path(path)),
			}
		};
		if module.kind != tg::module::Kind::Python {
			return Err(tg::error!("expected a python module"));
		}

		// Create the client.
		let client = self.create_client()?;

		// Connect the client.
		client.connect().await?;

		// Create the arg.
		let token = client.context().token();
		let url = client.url().to_string();
		let instance = tg::instance::dynamic::Instance::new(client);
		let main_runtime_handle = tokio::runtime::Handle::current();
		let arg = tangram_python::Arg {
			args: args_,
			cwd,
			env,
			export: args.export,
			instance,
			main_runtime_handle,
			module,
			token,
			url,
		};

		// Run.
		let tangram_python::Outcome {
			error,
			exit,
			output,
			..
		} = Self::spawn_thread(move || tangram_python::run(arg))
			.await
			.map_err(|error| tg::error!(!error, "the python thread failed"))?;

		// Write the serialized outcome to the file and mark it with an empty attribute.
		if let Ok(output_path) = std::env::var("TANGRAM_OUTPUT")
			&& (output.is_some() || error.is_some())
		{
			let outcome = tg::process::Outcome {
				error,
				exit,
				output,
			};
			let bytes = serde_json::to_vec(&outcome.to_data())
				.map_err(|error| tg::error!(!error, "failed to serialize the outcome"))?;
			std::fs::write(&output_path, bytes).map_err(
				|error| tg::error!(!error, path = %output_path, "failed to write the outcome"),
			)?;
			tg::file::xattrs::write_outcome(&output_path, b"")
				.map_err(|error| tg::error!(!error, "failed to write the outcome xattr"))?;
		}

		Ok(exit)
	}
}
