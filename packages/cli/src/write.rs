use {crate::Cli, tangram_client::prelude::*};

/// Write a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub bytes: Option<String>,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_write(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let output = if let Some(bytes) = args.bytes {
			let reader = std::io::Cursor::new(bytes);
			handle.write(reader).await?
		} else {
			let reader = crate::util::stdio::stdin();
			handle.write(reader).await?
		};
		Self::print_id(&output.blob);
		Ok(())
	}
}
