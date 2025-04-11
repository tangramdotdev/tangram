use crate::Cli;
use futures::{StreamExt as _, TryStreamExt as _, future};
use std::pin::pin;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;
use tokio::io::AsyncWriteExt as _;

/// Export processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub commands: bool,

	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub outputs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_export(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item {
			Either::Left(process) => Either::Left(process.id().clone()),
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
				Either::Right(object.id(&handle).await?)
			},
		};

		// Create the import stream.
		let stdin = tokio::io::stdin();
		let stream = tangram_http::sse::decode(tokio::io::BufReader::new(stdin))
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();

		// Export.
		let arg = tg::export::Arg {
			commands: args.commands,
			items: vec![item],
			remote,
			outputs: args.outputs,
			recursive: args.recursive,
		};
		let stream = handle.export(arg, stream).await?;

		// Write the stream.
		let mut stream = pin!(stream);
		let mut stdout = tokio::io::stdout();
		while let Some(event) = stream.try_next().await? {
			if let tg::export::Event::Item(item) = event {
				item.to_writer(&mut stdout).await?;
			}
		}
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
