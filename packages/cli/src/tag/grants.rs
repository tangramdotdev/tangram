use {crate::Cli, tangram_client::prelude::*};

/// List grants for a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub tag: tg::tag::Selector,
}

impl Cli {
	pub async fn command_tag_grants(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::tag::grants::list::Arg {
			location: args.location.get(),
		};
		let output = client
			.list_tag_grants(&args.tag, arg)
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag, "failed to list the grants"))?
			.ok_or_else(|| tg::error!(tag = %args.tag, "failed to find the tag"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
