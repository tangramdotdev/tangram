use std::pin::pin;

use crate::Cli;
use futures::stream;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;

/// Export processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_export(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Get reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item {
			Either::Left(process) => Either::Left(process),
			Either::Right(object) => {
				let object = if let Some(subpath) = &referent.subpath {
					let directory = object
						.try_unwrap_directory()
						.ok()
						.ok_or_else(|| tg::error!("expected a directory"))?;
					directory.get(&handle, subpath).await?.into()
				} else {
					object
				};
				Either::Right(object)
			},
		};
		let item = match item {
			Either::Left(process) => crate::viewer::Item::Process(process),
			Either::Right(object) => crate::viewer::Item::Value(object.into()),
		};

		// Create the export stream
		// let stdin = tokio::io::stdin();
		// // From SSE.
		// let stream = tangram_http::sse::decode(tokio_util::io::ReaderStream::new(stdin));
		// let stream = stream::try_unfold(stream, |mut reader| async move {
		// 	let Some(item) = tg::import::Event::from_reader(&mut reader).await? else {
		// 		return Ok(None);
		// 	};
		// 	Ok(Some((item, reader)))
		// })
		// .boxed();

		// // Produce the import stream
		// let arg = tg::export::Arg {
		// 	items: vec![item.into()],
		// 	remote,
		// };
		// let stream = handle.export(arg, stream).await?;

		// // Display the export stream.
		// // If TTY, progress. If not, events.
		// let mut stream = pin!(stream);
		// while let Some(event) = stream.try_next().await? {
		// 	if let tg::import::Event::Complete(_) = event {
		// 		let sse_event = tangram_http::sse::Event::try_from(event)?;
		// 		println!("{sse_event}");
		// 	}
		// }

		Ok(())
	}
}
