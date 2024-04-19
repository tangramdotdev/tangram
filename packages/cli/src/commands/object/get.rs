use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::AsyncWriteExt as _;

/// Get an object.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::object::Id,
}

impl Cli {
	pub async fn command_object_get(&self, args: Args) -> tg::Result<()> {
		let tg::object::GetOutput { bytes, .. } = self.handle.get_object(&args.id).await?;
		tokio::io::stdout()
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		Ok(())
	}
}
