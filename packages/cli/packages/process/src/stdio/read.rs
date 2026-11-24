use {
	futures::TryStreamExt as _, serde_with::serde_as, std::time::Duration, tangram_cli_core::Cli,
	tangram_client::prelude::*, tangram_util::serde::SeekFromNumberOrString,
	tokio::io::AsyncWriteExt as _,
};

/// Read a process's stdio.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(long)]
	pub length: Option<i64>,

	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(long)]
	pub position: Option<Position>,

	pub reference: tg::Reference,

	#[usage(long)]
	pub size: Option<u64>,

	#[usage(long, delimiter = ',', visible_alias = "stream")]
	pub streams: Vec<tg::process::stdio::Stream>,

	#[usage(flatten)]
	pub timeout: Timeout,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Timeout {
	#[usage(long = "timeout", overrides("--no-timeout"))]
	pub timeout: Option<humantime::Duration>,

	#[usage(long = "no-timeout", overrides("--timeout"))]
	pub no_timeout: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct Position(pub std::io::SeekFrom);

#[serde_as]
#[derive(serde::Deserialize)]
struct PositionArg {
	#[serde_as(as = "SeekFromNumberOrString")]
	position: std::io::SeekFrom,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let mut locations = args.locations;
	locations.set_from_reference_if_unset(&args.reference);
	let process =
		tangram_cli_reference::get_process_with_locations(cli, &args.reference, &locations).await?;
	let id = process.node.clone();
	let location = locations.get_for_options(&process);
	let process = tg::Process::<tg::Value>::with_referent(process);
	let streams = if args.streams.is_empty() {
		vec![
			tg::process::stdio::Stream::Stdout,
			tg::process::stdio::Stream::Stderr,
		]
	} else {
		args.streams
	};
	let options = tg::process::stdio::read::Options {
		length: args.length,
		location,
		position: args.position.map(|position| position.0),
		size: args.size,
		streams,
		timeout: args.timeout.get(),
	};
	let mut stdio = process
		.try_read_stdio_with_instance(&client, options)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to get the process stdio"))?
		.ok_or_else(|| tg::error!(%id, "failed to get the process stdio"))?;

	let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
	let mut stderr = tokio::io::BufWriter::new(tokio::io::stderr());
	while let Some(chunk) = stdio.try_next().await? {
		match chunk.stream {
			tg::process::stdio::Stream::Stdout => {
				stdout
					.write_all(&chunk.bytes)
					.await
					.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
			},
			tg::process::stdio::Stream::Stderr => {
				stderr
					.write_all(&chunk.bytes)
					.await
					.map_err(|error| tg::error!(!error, "failed to write to stderr"))?;
			},
			tg::process::stdio::Stream::Stdin => {
				return Err(tg::error!("invalid stdio stream"));
			},
		}
	}
	stdout
		.flush()
		.await
		.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;
	stderr
		.flush()
		.await
		.map_err(|error| tg::error!(!error, "failed to flush stderr"))?;

	Ok(())
}

impl Timeout {
	fn get(&self) -> Option<Duration> {
		if self.no_timeout {
			None
		} else {
			self.timeout.map(Into::into).or(Some(Duration::ZERO))
		}
	}
}

impl std::str::FromStr for Position {
	type Err = String;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		let value = serde_json::json!({ "position": value });
		let position = serde_json::from_value::<PositionArg>(value)
			.map_err(|error| format!("failed to parse the position: {error}"))?;
		Ok(Self(position.position))
	}
}
