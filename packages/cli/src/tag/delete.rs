use crate::Cli;
use tangram_client as tg;

/// Delete a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub tag: tg::Tag,
}

impl Cli {
	pub async fn command_tag_delete(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client.delete_tag(&args.tag).await?;
		Ok(())
	}
}
