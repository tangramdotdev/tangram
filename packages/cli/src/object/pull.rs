use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Pull an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub remote: Option<String>,

	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_pull(&self, args: Args) -> tg::Result<()> {
		let arg = tg::object::pull::Arg {
			remote: args.remote,
		};
		self.handle.pull_object(&args.object, arg).await?;
		Ok(())
	}
}
