use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Get a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,
}

impl Cli {
	pub async fn command_remote_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let remote = handle
			.try_get_remote(&args.name)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))?;
		println!("{}", remote.url);
		Ok(())
	}
}
