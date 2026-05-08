use {crate::Cli, tangram_client::prelude::*};

/// Delete a namespace.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub namespace: tg::Namespace,
}

impl Cli {
	pub async fn command_namespace_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client.delete_namespace(&args.namespace).await.map_err(
			|error| tg::error!(!error, namespace = %args.namespace, "failed to delete the namespace"),
		)?;
		Ok(())
	}
}
