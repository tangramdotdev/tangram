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

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_tag_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = handle.get_tag(&args.pattern).await?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
