use {crate::Cli, tangram_client::prelude::*};

/// List sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_sandbox_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::sandbox::list::Arg {
			location: args.locations.get(),
		};
		let output = client
			.list_sandboxes(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the sandboxes"))?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
