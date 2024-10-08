use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Pull an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[arg(short, long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_object_pull(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Pull the object.
		let arg = tg::object::pull::Arg { remote };
		let stream = handle.pull_object(&args.object, arg).await?;
		self.render_progress_stream(stream).await?;
		Ok(())
	}
}
