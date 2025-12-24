use {crate::Cli, tangram_client::prelude::*};

/// List remotes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_remote_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::remote::list::Arg::default();
		let output = handle
			.list_remotes(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the remotes"))?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
