use {crate::Cli, tangram_client::prelude::*};

/// Get a namespace.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub namespace: tg::Namespace,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_namespace_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.try_get_namespace(&args.namespace)
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to get the namespace"),
			)?
			.ok_or_else(
				|| tg::error!(namespace = %args.namespace, "failed to find the namespace"),
			)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
