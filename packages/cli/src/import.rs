use crate::Cli;
use futures::stream;
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

		// let stdin = tokio::io::stdin();

		// // Create the export stream
		// let stream = stream::try_unfold(stdin, |mut reader| async move {
		// 	let Some(item) = tg::export::Event::from_reader(&mut reader).await? else {
		// 		return Ok(None);
		// 	};
		// 	Ok(Some((item, reader)))
		// }); // boxed?

		// // Produce the import stream
		// let stream = handle.import(arg, stream).await?;

		//
		// Display the export stream.

		Ok(())
	}
}
