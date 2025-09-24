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

	#[arg(long)]
	pub pretty: Option<bool>,
}

impl Cli {
	pub async fn command_tag_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle.get_tag(&args.pattern).await?;
		Self::print_json(&output, args.pretty).await?;
		Ok(())
	}
}
