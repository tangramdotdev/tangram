use crate::Cli;
use tangram_client as tg;

use url::Url;

/// Put a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,

	#[arg(index = 2)]
	pub url: Url,
}

impl Cli {
	pub async fn command_remote_put(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let name = args.name;
		let url = args.url;
		let arg = tg::remote::put::Arg { url };
		client.put_remote(&name, arg).await?;
		Ok(())
	}
}
