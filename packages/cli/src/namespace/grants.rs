use {crate::Cli, tangram_client::prelude::*};

/// List grants for a namespace.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub namespace: tg::Namespace,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_namespace_grants(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_namespace_grants(tg::namespace::grants::Arg {
				namespace: args.namespace.clone(),
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to list the namespace grants"),
			)?
			.ok_or_else(
				|| tg::error!(namespace = %args.namespace, "failed to find the namespace"),
			)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
