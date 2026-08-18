use {
	crate::Cli,
	futures::{StreamExt as _, future, stream},
	tangram_client::prelude::*,
};

/// Write to a process's stdio.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[arg(long, value_delimiter = ',', visible_alias = "stream")]
	pub streams: Vec<tg::process::stdio::Stream>,
}

impl Cli {
	pub async fn command_process_stdio_write(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let mut location = args.location;
		location.set_from_reference_if_unset(&args.reference);
		let process = self
			.get_process_with_locations(&args.reference, &location)
			.await?;
		let id = process.node.clone();
		let location = location.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let [stream] = args.streams.as_slice() else {
			return Err(tg::error!("expected exactly one stdio stream"));
		};
		let stream = *stream;
		let options = tg::process::stdio::write::Options {
			location,
			streams: vec![stream],
		};
		let input = tangram_util::io::stdin()
			.map_err(|error| tg::error!(!error, "failed to open stdin"))?
			.filter_map(move |result| {
				future::ready(match result {
					Ok(bytes) if bytes.is_empty() => None,
					Ok(bytes) => Some(Ok(tg::process::stdio::read::Event::Chunk(
						tg::process::stdio::Chunk {
							bytes,
							position: None,
							stream,
						},
					))),
					Err(error) => Some(Err(tg::error!(!error, "failed to read stdin"))),
				})
			})
			.chain(stream::once(future::ok(
				tg::process::stdio::read::Event::End,
			)))
			.boxed();
		process
			.write_stdio_with_handle(&client, options, input)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to write process stdio"))?;
		Ok(())
	}
}
