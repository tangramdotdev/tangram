use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Push an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub object: tg::object::Id,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_object_push(&self, args: Args) -> tg::Result<()> {
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::object::push::Arg {
			remote,
		};
		self.handle.push_object(&args.object, arg).await?;
		Ok(())
	}
}
