use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Pull an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub object: tg::object::Id,

	#[arg(short, long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_object_pull(&self, args: Args) -> tg::Result<()> {
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());
		let arg = tg::object::pull::Arg { remote };
		self.handle.pull_object(&args.object, arg).await?;
		Ok(())
	}
}
