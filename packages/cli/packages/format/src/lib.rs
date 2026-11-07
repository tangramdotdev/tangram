use {
	futures::FutureExt as _, std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*,
};

/// Format.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub paths: Vec<PathBuf>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	for path in args.paths {
		// Canonicalize the path's parent.
		let path = tangram_util::fs::canonicalize_parent(&path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;

		// Format the selected path, including its nested module files.
		let arg = tg::format::Arg { path: path.clone() };
		client
			.format(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to format"))?;

		// Resolve only the explicitly selected package or module for its hook.
		let module_path = if path.is_dir() {
			let Some(name) = tg::module::try_get_root_module_file_name_with_instance(
				&client,
				tg::Either::Right(&path),
			)
			.await?
			else {
				continue;
			};
			path.join(name)
		} else if tg::module::is_module_path(&path) {
			path
		} else {
			continue;
		};
		let kind = tg::module::module_kind_for_path(&module_path)?;
		if matches!(
			kind,
			tg::module::Kind::JavaScript | tg::module::Kind::Python | tg::module::Kind::TypeScript
		) {
			// Discover local exports before resolving imports for a hook.
			let source = std::fs::read_to_string(&module_path)
				.map_err(|error| tg::error!(!error, "failed to read the module"))?;
			let module = tg::module::Data {
				kind,
				referent: tg::Referent::with_node(tg::module::data::Source::Path(
					module_path.clone(),
				)),
			};
			let interface = tangram_compiler::interface::module(&module, &source)?;
			if !interface.exports.contains_key("format") {
				continue;
			}
		} else if !kind.is_config() {
			continue;
		}
		let reference = module_path
			.to_str()
			.ok_or_else(|| tg::error!("invalid module path"))?
			.parse()?;
		let module = tangram_cli_reference::get_module(cli, &reference).await?;
		if kind.is_config() {
			let arg = tg::document::Arg {
				location: None,
				module: module.to_data(),
			};
			let document = client
				.document(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the module's exports"))?;
			if !document.exports.contains_key("format") {
				continue;
			}
		}

		// Run the selected module's format export on the host.
		let command =
			tg::command::Builder::with_module(module, Some("format".to_owned()), []).build()?;
		let command = tg::Referent::with_node(tg::Either::Right(command));
		let arg = tg::process::Arg {
			command: Some(command),
			sandbox: Some(tg::process::SandboxArg::Bool(false)),
			..Default::default()
		};
		tg::Process::<tg::Value>::run_with_instance(&client, arg)
			.boxed_local()
			.await
			.map_err(|error| tg::error!(!error, "failed to run the format export"))?;
	}

	Ok(())
}
