use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_bundle(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let artifact = self.get_artifact(&args.reference).await?;
		let artifact = tg::Artifact::with_referent(artifact);
		let artifact = tg::builtin::bundle_with_instance(&artifact, &client).await?;
		artifact
			.store_with_instance(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the artifact"))?;
		Cli::print_referent(&artifact.to_referent(), &args.print);

		Ok(())
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
