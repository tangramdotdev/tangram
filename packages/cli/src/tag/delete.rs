use {crate::Cli, tangram_client::prelude::*};

/// Delete a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Location,

	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub recursive: bool,
}

impl Cli {
	pub async fn command_tag_delete(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::tag::delete::Arg {
			location: args.location.get()?,
			pattern: args.pattern.clone(),
			recursive: args.recursive,
			replicate: None,
		};
		let output = handle.delete_tags(arg).await.map_err(
			|source| tg::error!(!source, pattern = %args.pattern, "failed to delete the tag"),
		)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
