use {crate::Cli, tangram_client::prelude::*};

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_bundle(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let artifact = self.resolve_artifact(&args.reference).await?;
		let artifact = tg::Artifact::with_referent(artifact);
		let artifact = tg::builtin::bundle_with_handle(&artifact, &client).await?;
		let id = artifact
			.store_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the artifact"))?;
		Self::print_display(id);

		Ok(())
	}
}
