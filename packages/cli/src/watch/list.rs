use {crate::Cli, tangram_client::prelude::*};

/// List watches.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_watch_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::watch::list::Arg::default();
		let output = handle.list_watches(arg).await?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
