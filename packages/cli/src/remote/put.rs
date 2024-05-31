use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;
use url::Url;

/// Put a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub name: String,
	pub url: Url,
}

impl Cli {
	pub async fn command_remote_put(&self, args: Args) -> tg::Result<()> {
		let name = args.name;
		let url = args.url;
		let arg = tg::remote::put::Arg { url };
		self.handle.put_remote(&name, arg).await?;
		Ok(())
	}
}
