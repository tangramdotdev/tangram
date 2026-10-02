use {crate::Cli, tangram_client::prelude::*};

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_bundle(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let artifact = self.get_artifact(&args.reference).await?;
		let artifact = tg::Artifact::with_referent(artifact);
		let artifact = tg::builtin::bundle_with_instance(&artifact, &client).await?;
		artifact
			.store_with_instance(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the artifact"))?;
		Self::print_referent(&artifact.to_referent(), &args.print);

		Ok(())
	}
}
