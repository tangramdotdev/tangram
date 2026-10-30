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
	if !module.kind.is_config() {
		return Err(tg::error!("expected a config module"));
	}

	// Create the client.
	let client = cli.create_client()?;
	client.connect().await?;

	// Get the config file.
	let artifact = module_artifact(&module)?;
	let file = artifact
		.try_unwrap_file()
		.map_err(|_| tg::error!("expected a config file"))?;

	// Parse the config in the format of the module's kind.
	let text = file.text_with_instance(&client).await?;
	let format = tg::module::config::Format::for_kind(module.kind).unwrap();
	let config = tg::module::Config::parse(&text, format)?;

	// Resolve the runtime, the dependencies, and the package, which a root config imports as `.`, relative to the config file.
	let referrer = module.to_data();
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

	let reference = ".".parse::<tg::Reference>()?;
	let package = if file
		.dependencies_with_instance(&client)
		.await?
		.contains_key(&reference)
	{
		let import = tg::module::Import {
			kind: Some(tg::module::Kind::Directory),
			reference,
		};
		let module = resolve(&client, &referrer, import).await?;
		Some(module_artifact(&module)?)
	} else {
		None
	};

	// Create the arg of the runtime's default export, which receives every export name, including `interface` and `check`.
	let mut arg = tg::value::Map::new();
	let export = args.export.unwrap_or_else(|| "default".to_owned());
	arg.insert("export".to_owned(), tg::Value::String(export));
	let args = config.args.into_iter().chain(args_).collect();
	arg.insert("args".to_owned(), tg::Value::Array(args));
	if let Some(package) = package {
		arg.insert("package".to_owned(), tg::Value::Object(package.into()));
	}
	arg.insert("dependencies".to_owned(), tg::Value::Map(dependencies));

	// Replace this process with the runtime's default export, so that it runs as a direct child of the caller's sandbox.
	let command = tg::command::Builder::with_module(
		runtime,
		Some("default".to_owned()),
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
