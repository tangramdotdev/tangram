use crate::Cli;
use std::{path::PathBuf, pin::Pin};
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;
use tokio::io::AsyncWrite;

/// Export an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub output: Option<PathBuf>,

	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub(crate) async fn command_object_export(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let reference = args
			.reference
			.clone()
			.unwrap_or_else(|| ".".parse().unwrap());

		// Get the remote.
		let remote = args
			.remote
			.clone()
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// Get the reference.
		let referent = self.get_reference(&reference).await?;
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let object = if let Some(subpath) = &referent.subpath {
			let directory = object
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			directory.get(&handle, subpath).await?.into()
		} else {
			object
		};
		let id = object.id(&handle).await?;

		// Create the writer.
		let mut writer: Pin<Box<dyn AsyncWrite + Send + 'static>> = if let Some(path) = &args.output
		{
			let writer = tokio::fs::File::create(path).await.map_err(
				|source| tg::error!(!source, %input = path.display(), "failed to open the file"),
			)?;
			Box::pin(writer)
		} else {
			let writer = tokio::io::stdout();
			Box::pin(writer)
		};

		// Create the stream.
		let arg = tg::object::export::Arg { remote };
		let mut reader = handle.export_object(&id, arg).await?;

		// Copy the reader to the writer.
		tokio::io::copy(&mut reader, &mut writer)
			.await
			.map_err(|source| tg::error!(!source, "failed to copy the reader to the writer"))?;

		Ok(())
	}
}
