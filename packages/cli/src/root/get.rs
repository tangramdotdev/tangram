use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Get a root.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
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
		println!("{}", root.build_or_object);
		Ok(())
	}
}
