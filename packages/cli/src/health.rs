use {crate::Cli, tangram_client::prelude::*};

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, value_delimiter = ',')]
	pub fields: Option<Vec<String>>,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_health(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::health::Arg {
			fields: args.fields,
		};
		let output = handle
			.health(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the health"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
