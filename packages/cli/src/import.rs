use {
	crate::Cli,
	futures::{StreamExt as _, TryStreamExt as _, stream},
	std::pin::pin,
	tangram_client::{self as tg, prelude::*},
	tokio::io::AsyncWriteExt as _,
};

/// Import processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub commands: bool,

	#[arg(long)]
	pub outputs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[allow(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_import(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Create the export stream.
		let stdin = crate::util::stdio::stdin();
		let stream = stream::try_unfold(stdin, |mut reader| async move {
			let Some(event) = tg::export::Event::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((event, reader)))
		})
		.boxed();

		// Import.
		let arg = tg::import::Arg {
			commands: args.commands,
			items: None,
			outputs: args.outputs,
			recursive: args.recursive,
			remote,
		};
		let stream = handle.import(arg, stream).await?;

		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			let event = tangram_http::sse::Event::try_from(event)?;
			stdout
				.write_all(event.to_string().as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
		}
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
