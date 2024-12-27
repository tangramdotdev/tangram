use std::{path::PathBuf, pin::Pin};
use tangram_client as tg;
use tg::Handle;
use tokio::io::AsyncRead;

use crate::Cli;

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub format: tg::artifact::archive::Format,

	#[arg(short, long)]
	pub compress: Option<tg::blob::compress::Format>,

	#[arg(short, long)]
	pub remote: Option<String>,

	pub input: Option<PathBuf>,
}

impl Cli {
	pub(crate) async fn command_object_import(&self, args: Args) -> tg::Result<()> {
		// Create the reader.
		let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = if let Some(input) = &args.input {
			let reader = tokio::fs::File::open(input).await.map_err(
				|source| tg::error!(!source, %input = input.display(), "failed to find input"),
			)?;
			Box::pin(reader)
		} else {
			let reader = tokio::io::stdin();
			Box::pin(reader)
		};

		let arg = tg::object::import::Arg {
			format: args.format,
			compress: args.compress,
			remote: args.remote,
		};

		// Create the stream.
		let stream = self.handle().await?.import_object(arg, reader).await?;

		// Render the stream.
		let object = self.render_progress_stream(stream).await?.object;

		// Print the object.
		println!("{object}");

		Ok(())
	}
}
