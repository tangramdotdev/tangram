use {
	crate::Cli, futures::TryStreamExt as _, serde_with::serde_as, tangram_client::prelude::*,
	tangram_util::serde::SeekFromNumberOrString, tokio::io::AsyncWriteExt as _,
};

/// Read a process's stdio.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub length: Option<i64>,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(long, value_parser = parse_seek_from)]
	pub position: Option<std::io::SeekFrom>,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(long)]
	pub size: Option<u64>,

	#[arg(long, value_delimiter = ',', visible_alias = "stream")]
	pub streams: Vec<tg::process::stdio::Stream>,

	#[arg(long)]
	pub wait: bool,
}

#[serde_as]
#[derive(serde::Deserialize)]
struct PositionArg {
	#[serde_as(as = "SeekFromNumberOrString")]
	position: std::io::SeekFrom,
}

impl Cli {
	pub async fn command_process_stdio_read(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let locations = args.locations.get();
		let process = tg::Process::<tg::Value>::new(
			args.process.clone(),
			locations.clone(),
			None,
			None,
			None,
			None,
		);
		let streams = if args.streams.is_empty() {
			vec![
				tg::process::stdio::Stream::Stdout,
				tg::process::stdio::Stream::Stderr,
			]
		} else {
			args.streams
		};
		let arg = tg::process::stdio::read::Arg {
			length: args.length,
			location: locations,
			position: args.position,
			size: args.size,
			streams,
			wait: args.wait,
		};
		let mut stdio = process
			.try_read_stdio_all(&client, arg)
			.await
			.map_err(
				|source| tg::error!(!source, id = %args.process, "failed to get the process stdio"),
			)?
			.ok_or_else(|| tg::error!(id = %args.process, "failed to get the process stdio"))?;

		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
		let mut stderr = tokio::io::BufWriter::new(tokio::io::stderr());
		while let Some(event) = stdio.try_next().await? {
			let tg::process::stdio::read::Event::Chunk(chunk) = event else {
				break;
			};
			match chunk.stream {
				tg::process::stdio::Stream::Stdout => {
					stdout
						.write_all(&chunk.bytes)
						.await
						.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
				},
				tg::process::stdio::Stream::Stderr => {
					stderr
						.write_all(&chunk.bytes)
						.await
						.map_err(|source| tg::error!(!source, "failed to write to stderr"))?;
				},
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("invalid stdio stream"));
				},
			}
		}
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;
		stderr
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stderr"))?;

		Ok(())
	}
}

fn parse_seek_from(value: &str) -> Result<std::io::SeekFrom, String> {
	let value = serde_json::json!({ "position": value });
	let position = serde_json::from_value::<PositionArg>(value)
		.map_err(|source| format!("failed to parse the position: {source}"))?;
	Ok(position.position)
}
