use crate::Cli;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use std::pin::pin;
use tangram_client::{self as tg, Handle as _};

/// Import processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_import(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Create the export stream.
		let stdin = tokio::io::stdin();
		let stream = stream::try_unfold(stdin, |mut reader| async move {
			let Some(item) = tg::export::Item::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		})
		.boxed();

		// Import.
		let arg = tg::import::Arg {
			items: vec![],
			remote,
		};
		let stream = handle.import(arg, stream).await?;

		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			let event = tangram_http::sse::Event::try_from(event)?;
			println!("{event}");
		}

		Ok(())
	}
}
