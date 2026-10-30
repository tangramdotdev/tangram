use {futures::FutureExt as _, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Check.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".", index = 1)]
	pub references: Vec<tg::Reference>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	// Get the modules.
	let modules = tangram_cli_reference::get_modules(cli, &args.references).await?;
	let referents: Vec<_> = modules
		.iter()
		.map(|module| module.referent.clone().replace(()).0)
		.collect();

	// Check the modules.
	let arg = tg::check::Arg {
		location: None,
		modules: modules.iter().map(tg::Module::to_data).collect(),
	};
	let output = client
		.check(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to check"))?;
	let success = !output
		.diagnostics
		.iter()
		.any(|diagnostic| diagnostic.severity.is_error());

	// Print the diagnostics.
	for diagnostic in output.diagnostics {
		let diagnostic: tg::Diagnostic = diagnostic.try_into()?;
		let mut diagnostic = tg::Referent::with_node(diagnostic);
		// Inherit from the first referent if available.
		if let Some(referent) = referents.first() {
			diagnostic.inherit(referent);
		}
		cli.print_diagnostic(diagnostic).await;
	}

	if !success {
		return Err(tg::error!("type checking failed"));
	}

	// Build the check exports, echoing their stdio.
	for module in output.checks {
		let module = tg::Module::try_from_data(module)?;
		let command =
			tg::command::Builder::with_module(module, Some("check".to_owned()), []).build()?;
		let command = tg::Referent::with_node(tg::Either::Right(command));
		let arg = tg::process::Arg {
			command: Some(command),
			echo: true,
			stderr: tg::process::Stdio::Log,
			stdin: tg::process::Stdio::Null,
			stdout: tg::process::Stdio::Log,
			..Default::default()
		};
		tg::Process::<tg::Value>::build_with_instance(&client, arg)
			.boxed_local()
			.await
			.map_err(|error| tg::error!(!error, "failed to build the check export"))?;
	}

	Ok(())
}
