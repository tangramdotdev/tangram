use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
	tangram_uri::Uri,
};

/// Put a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,

	#[arg(index = 2)]
	pub url: Uri,
}

impl Cli {
	pub async fn command_remote_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let name = args.name;
		let url = args.url;
		let arg = tg::remote::put::Arg { url };
		handle.put_remote(&name, arg).await?;
		Ok(())
	}
}
