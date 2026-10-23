use {crate::Component, tangram_client::prelude::*};

/// Delete a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub pattern: tg::specifier::Pattern,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(long)]
	pub recursive: bool,
}

impl Component<'_> {
	pub async fn command_tag_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::tag::delete::Arg {
			location: args.location.get(),
			pattern: args.pattern.clone(),
			recursive: args.recursive,
		};
		let output = client.delete_tags(arg).await.map_err(
			|error| tg::error!(!error, pattern = %args.pattern, "failed to delete the tag"),
		)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
