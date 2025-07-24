use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Create a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub bytes: Option<String>,
}

impl Cli {
	pub async fn command_blob(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let tg::blob::create::Output { blob, .. } = if let Some(bytes) = args.bytes {
			let reader = std::io::Cursor::new(bytes);
			handle.create_blob(reader).await?
		} else {
			let reader = tokio::io::BufReader::new(tokio::io::stdin());
			handle.create_blob(reader).await?
		};
		println!("{blob}");
		Ok(())
	}
}
