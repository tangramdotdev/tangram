use crate::Cli;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tokio::io::AsyncWriteExt;

/// Get an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	pub id: tg::object::Id,
}

impl Cli {
	#[allow(clippy::unused_async)]
	pub async fn command_get(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Get the data.
		let handle = tg::object::Handle::with_id(args.id);
		let data = handle.data(tg).await?;

		// Serialize the data.
		let data = data.serialize().wrap_err("Failed to serialize the data.")?;

		// Write the data to stdout.
		tokio::io::stdout()
			.write_all(&data)
			.await
			.wrap_err("Failed to write the data.")?;

		Ok(())
	}
}
