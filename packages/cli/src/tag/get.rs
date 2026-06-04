use {crate::Cli, tangram_client::prelude::*};

/// Get a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub tag: tg::tag::Selector,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_tag_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let tag = client
			.try_get_tag(&args.tag)
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag, "failed to get the tag"))?
			.ok_or_else(|| tg::error!("failed to find the tag"))?;
		self.print_serde(tag, args.print).await?;
		Ok(())
	}
}
