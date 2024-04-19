use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Push an object.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::object::Id,
}

impl Cli {
	pub async fn command_object_push(&self, args: Args) -> tg::Result<()> {
		self.handle.push_object(&args.id).await?;
		Ok(())
	}
}
