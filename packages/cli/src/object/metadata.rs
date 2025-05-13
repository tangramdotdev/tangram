use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Get object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[arg(long)]
	pub pretty: Option<bool>,
}

impl Cli {
	pub async fn command_object_metadata(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let metadata = handle.get_object_metadata(&args.object).await.map_err(
			|source| tg::error!(!source, %id = args.object, "failed to get the object metadata"),
		)?;
		Self::output_json(&metadata, args.pretty).await?;
		Ok(())
	}
}
