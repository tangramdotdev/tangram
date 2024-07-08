use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// List tags.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_tag_list(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// List the tags.
		let arg = tg::tag::list::Arg {
			pattern: args.pattern,
			remote,
		};
		let output = handle.list_tags(arg).await?;

		// Print the tags.
		for output in output.data {
			println!("{}", output.tag);
		}

		Ok(())
	}
}
