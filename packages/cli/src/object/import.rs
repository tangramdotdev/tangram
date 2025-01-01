use crate::Cli;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_futures::read::Ext as _;
use tg::Handle;

/// Import an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub input: Option<PathBuf>,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub(crate) async fn command_object_import(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// Create the reader.
		let reader = if let Some(path) = &args.input {
			tokio::fs::File::open(path)
				.await
				.map_err(
					|source| tg::error!(!source, %input = path.display(), "failed to open the file"),
				)?
				.boxed()
		} else {
			tokio::io::stdin().boxed()
		};

		// Create the stream.
		let arg = tg::object::import::Arg { remote };
		let stream = handle.import_object(arg, reader).await?;

		// Render the stream.
		let output = self.render_progress_stream(stream).await?;

		// Print the object.
		println!("{}", output.object);

		Ok(())
	}
}
