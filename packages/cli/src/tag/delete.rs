use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Delete a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub recursive: bool,

	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_tag_delete(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::tag::delete::Arg {
			pattern: args.pattern,
			recursive: args.recursive,
			remote,
		};
		let output = handle.delete_tag(arg).await?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
