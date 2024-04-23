use crate::Cli;
use tangram_client as tg;
use tg::Handle;

/// Get a root.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub name: String,
}

impl Cli {
	pub async fn command_root_get(&self, args: Args) -> tg::Result<()> {
		let root = self
			.handle
			.try_get_root(&args.name)
			.await?
			.ok_or_else(|| tg::error!("failed to find root"))?;
		println!("{}", root.id);
		Ok(())
	}
}
