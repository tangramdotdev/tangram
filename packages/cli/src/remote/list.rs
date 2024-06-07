use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// List remotes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_remote_list(&self, _args: Args) -> tg::Result<()> {
		let arg = tg::remote::list::Arg::default();
		let remotes = self.handle.list_remotes(arg).await?;
		for remote in remotes.data {
			println!("{} {}", remote.name, remote.url);
		}
		Ok(())
	}
}
