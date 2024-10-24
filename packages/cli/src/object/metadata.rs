use crate::Cli;
use tangram_client::{self as tg, handle::Ext as _};
/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_metadata(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let metadata = handle.get_object_metadata(&args.object).await.map_err(
			|source| tg::error!(!source, %id = args.object, "failed to get the object metadata"),
		)?;
		let metadata = serde_json::to_string_pretty(&metadata)
			.map_err(|source| tg::error!(!source, "failed to serialize the object metadata"))?;
		println!("{metadata}");
		Ok(())
	}
}
