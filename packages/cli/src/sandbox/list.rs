use {crate::Cli, tangram_client::prelude::*};

/// List sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_sandbox_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::sandbox::list::Arg::default();
		let output = handle
			.list_sandboxes(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the sandboxes"))?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
