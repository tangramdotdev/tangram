use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// List tags.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, default_value = "*")]
	pub pattern: tg::tag::Pattern,

	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(long)]
	pub recursive: bool,

	#[expect(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,

	#[arg(long)]
	pub reverse: bool,
}

impl Cli {
	pub async fn command_tag_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::tag::list::Arg {
			length: None,
			pattern: args.pattern,
			recursive: args.recursive,
			remote,
			reverse: args.reverse,
		};
		let output = handle.list_tags(arg).await?;
		Self::print_json(&output, args.pretty).await?;
		Ok(())
	}
}
