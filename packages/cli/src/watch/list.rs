use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// List watches.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_watch_list(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::watch::list::Arg::default();
		let watches = handle.list_watches(arg).await?;
		for watch in watches.data {
			println!("{}", watch.path.display());
		}
		Ok(())
	}
}
