use crate::Cli;
use tangram_client as tg;

/// List remotes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_remote_list(&self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::remote::list::Arg::default();
		let remotes = client.list_remotes(arg).await?;
		for remote in remotes.data {
			println!("{} {}", remote.name, remote.url);
		}
		Ok(())
	}
}
