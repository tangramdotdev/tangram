use crate::Cli;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use std::{io::IsTerminal, pin::pin};
use tangram_client::{self as tg, Handle};

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

		// Create the export stream
		let stdin = tokio::io::stdin();
		let stream = stream::try_unfold(stdin, |mut reader| async move {
			let Some(item) = tg::export::Event::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		})
		.boxed();

		// Produce the import stream
		let arg = tg::import::Arg {
			items: vec![],
			remote,
		};
		let stream = handle.import(arg, stream).await?;

		// Display the export stream.
		let mut stream = pin!(stream);
		let is_terminal = std::io::stdout().is_terminal();
		if is_terminal {
			let stream = stream.filter_map(|event| async move {
				match event {
					Ok(tg::import::Event::Progress(progress)) => Some(Ok(progress)),
					Ok(tg::import::Event::Complete(_)) => None,
					Err(e) => Some(Err(e)),
				}
			});
			self.render_progress_stream(stream).await?;
		} else {
			while let Some(event) = stream.try_next().await? {
				if let tg::import::Event::Complete(_) = event {
					let sse_event = tangram_http::sse::Event::try_from(event)?;
					println!("{sse_event}");
				}
			}
		}

		Ok(())
	}
}
