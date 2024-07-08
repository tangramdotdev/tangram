use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Get a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,
}

impl Cli {
	pub async fn command_tag_get(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let tag = handle
			.try_get_tag(&args.pattern)
			.await?
			.ok_or_else(|| tg::error!("failed to find the tag"))?;
		println!("{}", tag.item);
		Ok(())
	}
}
