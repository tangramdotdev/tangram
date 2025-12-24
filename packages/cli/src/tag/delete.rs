use {crate::Cli, tangram_client::prelude::*};

/// Delete a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub recursive: bool,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_tag_delete(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::tag::delete::Arg {
			local: args.local.local,
			pattern: args.pattern.clone(),
			recursive: args.recursive,
			remotes: args.remotes.remotes,
		};
		let output = handle.delete_tag(arg).await.map_err(
			|source| tg::error!(!source, pattern = %args.pattern, "failed to delete the tag"),
		)?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
