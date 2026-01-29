use {crate::Cli, tangram_client::prelude::*};

/// Write a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub bytes: Option<String>,

	#[command(flatten)]
	pub cache_pointers: crate::checkin::CachePointers,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_write(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::write::Arg {
			cache_pointers: args.cache_pointers.get_option(),
		};
		let output = if let Some(bytes) = args.bytes {
			let reader = std::io::Cursor::new(bytes);
			handle
				.write(arg, reader)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the blob"))?
		} else {
			let reader = crate::util::stdio::stdin();
			handle
				.write(arg, reader)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the blob"))?
		};
		Self::print_id(&output.blob);
		Ok(())
	}
}
