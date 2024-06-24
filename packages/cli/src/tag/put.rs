use crate::Cli;
use tangram_client as tg;

/// Put a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub force: bool,

	#[arg(index = 2)]
	pub reference: tg::Reference,

	#[arg(index = 1)]
	pub tag: tg::Tag,
}

impl Cli {
	pub async fn command_tag_put(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let tag = args.tag;
		let force = args.force;
		let item = args.reference.get(&client).await?;
		let arg = tg::tag::put::Arg { force, item };
		client.put_tag(&tag, arg).await?;
		Ok(())
	}
}
