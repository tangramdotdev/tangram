use crate::Cli;
use tangram_client as tg;

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
		let client = self.client().await?;
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::tag::list::Arg {
			pattern: args.pattern,
			remote,
		};
		let output = client.list_tags(arg).await?;
		for output in output.data {
			println!("{}", output.tag);
		}
		Ok(())
	}
}
