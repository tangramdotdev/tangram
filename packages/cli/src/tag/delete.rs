use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Delete a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub tag: tg::Tag,
}

impl Cli {
	pub async fn command_tag_delete(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		handle.delete_tag(&args.tag).await?;
		Ok(())
	}
}
