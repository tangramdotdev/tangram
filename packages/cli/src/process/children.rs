use {
	crate::Cli,
	futures::{StreamExt as _, TryStreamExt as _},
	serde_with::serde_as,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_util::serde::SeekFromNumberOrString,
};

/// Get a process's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub length: Option<u64>,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(long, value_parser = parse_seek_from)]
	pub position: Option<std::io::SeekFrom>,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::Reference,

	#[arg(long)]
	pub size: Option<u64>,

	#[command(flatten)]
	pub timeout: Timeout,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Timeout {
	#[arg(id = "children.timeout.timeout", long = "timeout", overrides_with = "children.timeout.no_timeout", value_parser = humantime::parse_duration)]
	pub timeout: Option<Duration>,

	#[arg(
		id = "children.timeout.no_timeout",
		long = "no-timeout",
		overrides_with = "children.timeout.timeout"
	)]
	pub no_timeout: bool,
}

#[serde_as]
#[derive(serde::Deserialize)]
struct PositionArg {
	#[serde_as(as = "SeekFromNumberOrString")]
	position: std::io::SeekFrom,
}

impl Timeout {
	fn get(&self) -> Option<Duration> {
		if self.no_timeout {
			None
		} else {
			self.timeout.or(Some(Duration::ZERO))
		}
	}
}

impl Cli {
	pub async fn command_process_children(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let locations = args.locations.get();
		let process = self.resolve_process(&args.process).await?;
		let id = process.node;
		let tokens = process.options.tokens;
		let process = tg::Process::<tg::Value>::new(
			id.clone(),
			tg::process::Options {
				location: locations.clone(),
				tokens: tokens.clone(),
				..Default::default()
			},
		);
		let arg = tg::process::children::get::Arg {
			length: args.length,
			location: locations,
			position: args.position,
			size: args.size,
			timeout: args.timeout.get(),
			tokens: tg::authorization::Tokens::default(),
		};
		let stream = process
			.children_with_handle(&client, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process children"))?
			.map_ok(|child| child.to_data());
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}

fn parse_seek_from(value: &str) -> Result<std::io::SeekFrom, String> {
	let value = serde_json::json!({ "position": value });
	let position = serde_json::from_value::<PositionArg>(value)
		.map_err(|error| format!("failed to parse the position: {error}"))?;
	Ok(position.position)
}
