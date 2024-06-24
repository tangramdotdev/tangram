use crate::Cli;
use crossterm::{style::Stylize as _, tty::IsTty as _};
use tangram_client::{self as tg, handle::Ext as _};
use tokio::io::AsyncWriteExt as _;

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_get(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let tg::object::get::Output { bytes, metadata } = client.get_object(&args.object).await?;
		if let Some(count) = metadata.count {
			eprintln!("{} count {count}", "info".blue().bold());
		}
		if let Some(weight) = metadata.weight {
			eprintln!("{} weight {weight}", "info".blue().bold());
		}
		let mut stdout = tokio::io::stdout();
		if stdout.is_tty() && !matches!(args.object, tg::object::Id::Leaf(_)) {
			let json = serde_json::from_slice::<serde_json::Value>(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
			let bytes = serde_json::to_vec_pretty(&json)
				.map_err(|source| tg::error!(!source, "failed to serialize the object"))?;
			stdout
				.write_all(&bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the data"))?;
			stdout
				.write_all(b"\n")
				.await
				.map_err(|source| tg::error!(!source, "failed to write"))?;
		} else {
			stdout
				.write_all(&bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		}
		Ok(())
	}
}
