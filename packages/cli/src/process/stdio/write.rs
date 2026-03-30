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
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	#[arg(long, value_delimiter = ',', visible_alias = "stream")]
	pub streams: Vec<tg::process::stdio::Stream>,
}

impl Cli {
	pub async fn command_process_stdio_write(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let process = tg::Process::new(args.process.clone(), None, None, None, None);
		let [stream] = args.streams.as_slice() else {
			return Err(tg::error!("expected exactly one stdio stream"));
		};
		let stream = *stream;
		let arg = tg::process::stdio::write::Arg {
			local: args.local.get(),
			remotes: args.remotes.get(),
			streams: vec![stream],
		};
		let input = tangram_util::io::stdin()
			.map_err(|source| tg::error!(!source, "failed to open stdin"))?
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
		process.write_stdio_all(&handle, arg, input).await.map_err(
			|source| tg::error!(!source, id = %args.process, "failed to write process stdio"),
		)?;
		Ok(())
	}
}
