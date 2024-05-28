use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Push an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub remote: Option<String>,

	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_push(&self, args: Args) -> tg::Result<()> {
		let arg = tg::object::push::Arg {
			remote: args.remote,
		};
		self.handle.push_object(&args.object, arg).await?;
		Ok(())
	}
}
