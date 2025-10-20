use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Write a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub bytes: Option<String>,
}

impl Cli {
	pub async fn command_write(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let tg::write::Output { blob, .. } = if let Some(bytes) = args.bytes {
			let reader = std::io::Cursor::new(bytes);
			handle.write(reader).await?
		} else {
			let reader = crate::util::stdio::stdin();
			handle.write(reader).await?
		};
		println!("{blob}");
		Ok(())
	}
}
