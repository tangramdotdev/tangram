use crate::Cli;
use tangram_client as tg;

/// Get a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub name: String,
}

impl Cli {
	pub async fn command_remote_get(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let remote = client
			.try_get_remote(&args.name)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))?;
		println!("{}", remote.url);
		Ok(())
	}
}
