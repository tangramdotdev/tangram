use crate::Cli;
use tangram_client as tg;

/// Get a root.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub name: String,
}

impl Cli {
	pub async fn command_root_get(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let root = client
			.try_get_root(&args.name)
			.await?
			.ok_or_else(|| tg::error!("failed to find the root"))?;
		println!("{}", root.item);
		Ok(())
	}
}
