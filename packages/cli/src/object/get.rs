use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::AsyncWriteExt as _;

/// Get an object.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_get(&self, args: Args) -> tg::Result<()> {
		let tg::object::get::Output { bytes, .. } = self.handle.get_object(&args.object).await?;
		tokio::io::stdout()
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		Ok(())
	}
}
