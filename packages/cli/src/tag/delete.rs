use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Delete a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub tag: tg::Tag,
}

impl Cli {
	pub async fn command_tag_delete(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		handle.delete_tag(&args.tag).await?;
		Ok(())
	}
}
