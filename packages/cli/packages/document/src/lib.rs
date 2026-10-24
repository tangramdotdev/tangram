use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	/// Generate the documentation for the runtime.
	#[arg(long)]
	pub runtime: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	// Get the module.
	let module = if args.runtime {
		tg::module::Data {
			kind: tg::module::Kind::TypeScriptDeclaration,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				"tangram.d.ts".into(),
			)),
		}
	} else {
		tangram_cli_get::get_module(cli, &args.reference)
			.await?
			.to_data()
	};

	// Document the module.
	let arg = tg::document::Arg {
		location: args.location.get(),
		module,
	};
	let output = client
		.document(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to get the document"))?;

	// Print the document.
	cli.print_serde(output, args.print).await?;

	Ok(())
}
