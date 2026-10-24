use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

pub async fn command_bundle(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let artifact = tangram_cli_get::get_artifact(cli, &args.reference).await?;
	let artifact = tg::Artifact::with_referent(artifact);
	let artifact = tg::builtin::bundle_with_instance(&artifact, &client).await?;
	artifact
		.store_with_instance(&client)
		.await
		.map_err(|error| tg::error!(!error, "failed to store the artifact"))?;
	Cli::print_referent(&artifact.to_referent(), &args.print);

	Ok(())
}
