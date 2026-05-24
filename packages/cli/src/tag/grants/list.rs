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
	pub tag: tg::Tag,
}

impl Cli {
	pub async fn command_tag_grants_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_tag_grants(tg::tag::grants::list::Arg {
				location: args.location.get(),
				tag: args.tag.clone(),
			})
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag, "failed to list the tag grants"))?
			.ok_or_else(|| tg::error!(tag = %args.tag, "failed to find the tag"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
