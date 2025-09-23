use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Get a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,
}

impl Cli {
	pub async fn command_tag_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let tag = handle.get_tag(&args.pattern).await?;
		let item = tag
			.item
			.ok_or_else(|| tg::error!("the tag does not have an item"))?;
		println!("{item}");
		Ok(())
	}
}
