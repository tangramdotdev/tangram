use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// List remotes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_remote_list(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::remote::list::Arg::default();
		let remotes = handle.list_remotes(arg).await?;
		for remote in remotes.data {
			println!("{} {}", remote.name, remote.url);
		}
		Ok(())
	}
}
