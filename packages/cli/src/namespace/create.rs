use {crate::Cli, tangram_client::prelude::*};

/// Create a namespace.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub namespace: tg::Namespace,
}

impl Cli {
	pub async fn command_namespace_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client.create_namespace(&args.namespace).await.map_err(
			|error| tg::error!(!error, namespace = %args.namespace, "failed to create the namespace"),
		)?;
		Ok(())
	}
}
