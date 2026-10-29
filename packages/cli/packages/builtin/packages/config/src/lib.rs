use {std::collections::BTreeMap, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Run an export of a config module with the module's runtime.
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

	/// Run a named export of the runtime, such as `interface` or `check`, rather than the runtime's default export with the module's export.
	#[arg(conflicts_with = "export", long)]
	pub runtime_export: Option<String>,

	#[arg(index = 1, value_parser = tg::value::parse)]
	pub module: tg::Value,

	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Get the args.
	let mut args_ = Vec::new();
	let mut matches = &cli.matches;
	while let Some((_, matches_)) = matches.subcommand() {
		matches = matches_;
	}
	let arg_string_indices = matches.indices_of("arg_strings").unwrap_or_default();
	let arg_value_indices = matches.indices_of("arg_values").unwrap_or_default();
	let mut indexed = Vec::new();
	for (index, value) in arg_string_indices.zip(args.arg_strings) {
		indexed.push((index, tg::Value::String(value)));
	}
	for (index, value) in arg_value_indices.zip(args.arg_values) {
		let value = value
			.parse()
			.map_err(|error| tg::error!(!error, arg = %value, "failed to parse the arg"))?;
		indexed.push((index, value));
	}
	indexed.sort_by_key(|&(index, _)| index);
	args_.extend(indexed.into_iter().map(|(_, value)| value));
	args_.extend(args.trailing.into_iter().map(tg::Value::String));

	// Get the module.
	let module = args
		.module
		.try_unwrap_module()
		.map_err(|_| tg::error!("expected a config module"))?;
	if module.kind != tg::module::Kind::Config {
		return Err(tg::error!("expected a config module"));
	}

	// Create the client.
	let client = cli.create_client()?;
	client.connect().await?;

	// Get the config file, and the package if the module is a package directory.
	let artifact = module_artifact(&module)?;
	let (file, edge, package, name) = match artifact {
		tg::Artifact::Directory(directory) => {
			let name = tg::module::try_get_root_module_file_name_with_instance(
				&client,
				tg::Either::Left(&directory),
			)
			.await?
			.ok_or_else(|| tg::error!("failed to find the root module of the package"))?;
			let edge = directory
				.get_entry_edge_with_instance(&client, name)
				.await?;
			let file = tg::Artifact::with_edge(edge.clone())?
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected the root module to be a file"))?;
			let edge = match edge {
				tg::graph::Edge::Index(index) => tg::graph::Edge::Index(index),
				tg::graph::Edge::Object(artifact) => tg::graph::Edge::Object(artifact.into()),
				tg::graph::Edge::Pointer(pointer) => tg::graph::Edge::Pointer(pointer),
			};
			(file, edge, Some(directory), Some(name.to_owned()))
		},
		tg::Artifact::File(file) => {
			let name = module
				.referent
				.path()
				.and_then(|path| path.file_name())
				.and_then(|name| name.to_str())
				.map(ToOwned::to_owned);
			let tg::module::Source::Edge(edge) = module.referent.node().clone() else {
				unreachable!()
			};
			(file, edge, None, name)
		},
		tg::Artifact::Symlink(_) => return Err(tg::error!("expected a config file")),
	};

	// Parse the config, in the format that the file name implies, or in the first format that it parses in.
	let text = file.text_with_instance(&client).await?;
	let format = name
		.as_deref()
		.and_then(tg::module::config::Format::for_path);
	let config = parse(&text, format)?;

	// Resolve the runtime and the dependencies relative to the config file.
	let source = tg::module::Source::Edge(edge);
	let mut referent = module.referent.clone().map(|_| source);
	referent.options.path = name.map(Into::into);
	let referrer = tg::Module {
		kind: tg::module::Kind::Config,
		referent,
	}
	.to_data();
	let import = config.runtime.import()?;
	let runtime = resolve(&client, &referrer, import).await?;
	let mut dependencies = BTreeMap::new();
	for (name, dependency) in &config.dependencies {
		let mut import = dependency.import()?;
		import.kind = Some(tg::module::Kind::Artifact);
		let module = resolve(&client, &referrer, import).await?;
		let artifact = module_artifact(&module)?;
		let mut options = BTreeMap::new();
		if let Some(path) = module.referent.path() {
			options.insert(
				"path".to_owned(),
				tg::Value::String(path.to_string_lossy().into_owned()),
			);
		}
		if let Some(tag) = module.referent.tag() {
			options.insert("tag".to_owned(), tg::Value::String(tag.to_string()));
		}
		let referent = tg::value::Map::from([
			("item".to_owned(), tg::Value::Object(artifact.into())),
			("options".to_owned(), tg::Value::Map(options)),
		]);
		dependencies.insert(name.clone(), tg::Value::Map(referent));
	}

	// Create the arg of the runtime's export, which includes the module's export when it is the runtime's default export.
	let mut arg = tg::value::Map::new();
	let runtime_export = args.runtime_export.unwrap_or_else(|| "default".to_owned());
	if runtime_export == "default" {
		let export = args.export.unwrap_or_else(|| "default".to_owned());
		arg.insert("export".to_owned(), tg::Value::String(export));
	}
	let args = config.args.into_iter().chain(args_).collect();
	arg.insert("args".to_owned(), tg::Value::Array(args));
	if let Some(package) = package {
		arg.insert("package".to_owned(), tg::Value::Object(package.into()));
	}
	arg.insert("dependencies".to_owned(), tg::Value::Map(dependencies));

	// Replace this process with the runtime's export, so that it runs as a direct child of the caller's sandbox.
	let command = tg::command::Builder::with_module(
		runtime,
		Some(runtime_export),
		[tg::command::Value::Value(tg::Value::Map(arg))],
	)
	.build()?;
	let env = tg::process::env()?;
	let command = tg::Referent::with_node(tg::Either::Right(command));
	let arg = tg::process::Arg {
		command: Some(command),
		env,
		..Default::default()
	};
	tg::Process::<tg::Value>::exec_with_instance(&client, arg).await?;

	Ok(())
}

/// Get the artifact that a module refers to.
fn module_artifact(module: &tg::Module) -> tg::Result<tg::Artifact> {
	let tg::module::Source::Edge(edge) = module.referent.node() else {
		return Err(tg::error!("expected a module that refers to an object"));
	};
	match edge.clone() {
		tg::graph::Edge::Index(_) => Err(tg::error!("missing graph")),
		tg::graph::Edge::Object(object) => {
			tg::Artifact::try_from(object).map_err(|_| tg::error!("expected an artifact"))
		},
		tg::graph::Edge::Pointer(pointer) => Ok(tg::Artifact::with_pointer(pointer)),
	}
}

/// Parse a config in a format, or in the first format that it parses in if the format is not known.
fn parse(text: &str, format: Option<tg::module::config::Format>) -> tg::Result<tg::module::Config> {
	if let Some(format) = format {
		return tg::module::Config::parse(text, format);
	}
	let mut error = None;
	for format in [
		tg::module::config::Format::Json,
		tg::module::config::Format::Toml,
		tg::module::config::Format::Yaml,
	] {
		match tg::module::Config::parse(text, format) {
			Ok(config) => return Ok(config),
			Err(error_) => error = Some(error_),
		}
	}
	Err(error.unwrap())
}

/// Resolve an import of a config module.
async fn resolve(
	client: &tg::Client,
	referrer: &tg::module::Data,
	import: tg::module::Import,
) -> tg::Result<tg::Module> {
	let arg = tg::module::resolve::Arg {
		import,
		referrer: Some(referrer.clone()),
	};
	let output = client
		.resolve_module(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to resolve the import of the config"))?;
	tg::Module::try_from_data(output.module)
}
